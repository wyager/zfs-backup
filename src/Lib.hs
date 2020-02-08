module Lib where

import Data.Word (Word64)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Char8 as BS
import Data.Time.Clock (UTCTime)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import qualified Data.Attoparsec.ByteString.Char8 as A
import qualified Data.Attoparsec.ByteString as AW
import Control.Applicative (many, (<|>))
import qualified System.Process.Typed as P
import System.Exit (ExitCode(ExitSuccess,ExitFailure))
import System.IO (Handle, hClose)
import GHC.Conc (STM, atomically)
import GHC.Generics (Generic)
import Options.Generic (ParseRecord, ParseField, ParseFields, readField, getRecord)
import qualified Options.Applicative as Opt
import Data.Bifunctor (first)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Control.Exception as Ex
import Control.Monad (zipWithM, when)
import Data.List (intercalate)

data ListError = CommandError ByteString | ZFSListParseError String deriving (Show, Ex.Exception)

data Command 
    = List {
        remote :: Maybe SSHSpec
    } 
    | Copy {
        srcFS :: FilesystemName,
        srcRemote :: Maybe SSHSpec,
        dstFS :: FilesystemName,
        dstRemote :: Maybe SSHSpec,
        sendCompressed :: Bool,
        sendRaw :: Bool,
        dryRun :: Bool
    }
    deriving (Generic, ParseRecord, Show)

speedTest :: IO ()
speedTest = do
    let sndProc = P.setStdin P.closed $ P.setStdout P.createPipe $ "dd bs=1m if=/dev/zero count=10000"
    let rcvProc = P.setStdout P.closed $ P.setStdin P.createPipe $ "dd bs=1m of=/dev/null"
    oneStep (const $ return ()) sndProc rcvProc

someFunc :: IO ()
someFunc = speedTest

runCommand :: IO ()
runCommand = do
    command <- getRecord "Tool"
    case command of
        List host -> do
            result <- listWith $ case host of
                    Nothing -> localCmd
                    Just spec -> sshCmd spec
            print result
        Copy{..} -> do
            let srcList = maybe localCmd sshCmd srcRemote
                dstList = maybe localCmd sshCmd dstRemote
            srcSnaps <- either Ex.throw (return . snapshots) =<< listWith srcList
            dstSnaps <- either Ex.throw (return . snapshots) =<< listWith dstList
            case copyPlan srcFS srcSnaps dstFS dstSnaps of
                Left err -> print err
                Right plan -> if dryRun
                    then putStrLn (showShell srcRemote dstRemote (SendOptions sendCompressed sendRaw) plan)
                    else executeCopyPlan srcRemote dstRemote (SendOptions sendCompressed sendRaw) plan

oneStep ::  (Int -> IO ()) -> P.ProcessConfig () Handle () ->  P.ProcessConfig Handle () () -> IO ()
oneStep progress sndProc rcvProc = do
    print sndProc
    print rcvProc
    P.withProcessWait_ rcvProc $ \rcv ->
        P.withProcessWait_ sndProc $ \send -> do
            let sndHdl = P.getStdout send
            let rcvHdl = P.getStdin rcv
            let go = do
                    -- hGetSome is faster but maxes out CPU
                    chunk <- BS.hGet sndHdl 0xFFFF
                    BS.hPut rcvHdl chunk
                    if BS.null chunk
                        then hClose rcvHdl
                        else progress (BS.length chunk) >> go
            go

executeCopyPlan :: Maybe SSHSpec -> Maybe SSHSpec -> SendOptions -> CopyPlan -> IO ()
executeCopyPlan sndSpec rcvSpec sndOpts plan = case plan of
    Nada -> putStrLn "Nothing to do"
    FullCopy _guid snap dstFs -> do
        let (sndExe,sndArgs) = sendCommand sndSpec sndOpts (Left snap)
        let (rcvExe,rcvArgs) = recCommand rcvSpec dstFs (Left snap)
        let sndProc = P.setStdin P.closed $ P.setStdout P.createPipe $ P.proc sndExe sndArgs
        let rcvProc = P.setStdout P.closed $ P.setStdin P.createPipe $ P.proc rcvExe rcvArgs
        oneStep (const $ return ()) sndProc rcvProc
    Incremental start steps -> flip mapM_ steps $ \step -> do
        let (sndExe,sndArgs) = sendCommand sndSpec sndOpts (Right step)
        let (rcvExe,rcvArgs) = recCommand rcvSpec (snapshotFSOf start) (Right step)
        let sndProc = P.setStdin P.closed $ P.setStdout P.createPipe $ P.proc sndExe sndArgs
        let rcvProc = P.setStdout P.closed $ P.setStdin P.createPipe $ P.proc rcvExe rcvArgs
        oneStep (const $ return ()) sndProc rcvProc

listWith :: P.ProcessConfig () () () -> IO (Either ListError [Object])
listWith cmd = do
    output <- P.withProcessWait (allOutputs cmd) $ \proc -> do
        bytes <- fmap LBS.toStrict $ atomically $ P.getStdout proc
        err <- fmap LBS.toStrict $ atomically $ P.getStderr proc
        P.waitExitCode proc >>= \case
            ExitSuccess -> return (Right bytes)
            ExitFailure _i -> return $ Left $ CommandError err
    return $ output >>= first ZFSListParseError . A.parseOnly objects 

newtype Size = Size Word64 deriving newtype (Eq, Ord, Show, Num)

newtype PoolName = PoolName ByteString deriving newtype (Eq, Ord, Show)

newtype GUID = GUID Word64 deriving newtype (Eq, Ord, Show)

data ObjectMeta = ObjectMeta 
    { creationOf :: !UTCTime
    , guidOf :: !GUID
    , referencedOf :: !Size
    , usedOf :: !Size
    } deriving (Eq, Ord, Show)

data Object = Filesystem FilesystemName ObjectMeta
            | Volume
            | Snapshot SnapshotName ObjectMeta
            deriving (Eq, Ord, Show)


newtype FilesystemName = FilesystemName ByteString 
    deriving stock (Eq, Ord, Generic)
    deriving anyclass ParseRecord

filesystemNameP :: A.Parser FilesystemName
filesystemNameP = FilesystemName <$> A.takeWhile (not . A.inClass " @\t")

instance Show FilesystemName where
    show (FilesystemName bs) = BS.unpack bs

instance ParseField FilesystemName where
    readField = Opt.eitherReader (first ("Parse error: " ++ ) . A.parseOnly (filesystemNameP <* A.endOfInput) . BS.pack)
deriving anyclass instance ParseFields FilesystemName

data SnapshotName = SnapshotName {snapshotFSOf :: FilesystemName, snapshotNameOf :: ByteString} deriving (Eq,Ord)
instance Show SnapshotName where
    show (SnapshotName fs snap) = show fs ++ "@" ++ BS.unpack snap

snapshotNameP :: A.Parser SnapshotName
snapshotNameP = do
    snapshotFSOf <- filesystemNameP
    snapshotNameOf <- "@" *> A.takeWhile (not . A.inClass " \t")
    return SnapshotName{..}


instance ParseField SnapshotName where
    readField = Opt.eitherReader (first ("Parse error: " ++ ) . A.parseOnly (snapshotNameP <* A.endOfInput) . BS.pack)



object :: A.Parser Object
object = (fs <|> vol <|> snap) <* A.endOfLine
    where
    fs = "filesystem" *> (Filesystem <$> t filesystemNameP <*> t meta)
    vol = "volume" *> (Volume <$ AW.takeTill A.isEndOfLine)
    snap = "snapshot" *> (Snapshot <$> t snapshotNameP <*> t meta)
    meta = ObjectMeta <$> creation <*> t guid <*> t size <*> t size
    t x = A.char '\t' *> x
    creation = seconds <$> A.decimal
    guid = GUID <$> A.decimal
    size = Size <$> A.decimal

objects :: A.Parser [Object]
objects = many object <* A.endOfInput



seconds :: Word64 -> UTCTime
seconds = posixSecondsToUTCTime . fromIntegral

class PoolM m where 
    contents :: PoolName -> m [Object]

shellCmd :: String
shellCmd = "zfs list -Hp -t all -o type,name,creation,guid,referenced,used" 

localCmd :: P.ProcessConfig () () ()
localCmd = P.shell shellCmd

sshCmd :: SSHSpec -> P.ProcessConfig () () ()
sshCmd spec = P.shell $ "ssh " ++ show spec ++ " " ++ shellCmd

allOutputs :: P.ProcessConfig () () () -> P.ProcessConfig () (STM LBS.ByteString) (STM LBS.ByteString)
allOutputs command = P.setStdin P.closed $ P.setStdout P.byteStringOutput $ P.setStderr P.byteStringOutput command


data SSHSpec = SSHSpec {
    user :: Maybe ByteString,
    host :: ByteString
} deriving (Generic)

instance Show SSHSpec where
    show SSHSpec{..} = case user of
        Nothing -> BS.unpack host
        Just usr -> show usr ++ "@" ++ BS.unpack host

instance ParseField SSHSpec where
    readField = Opt.eitherReader (first ("Parse error: " ++ ) . A.parseOnly (sshSpecP <* A.endOfInput) . BS.pack)

sshSpecP :: A.Parser SSHSpec
sshSpecP = do
    let reserved = A.inClass " @/"
    user <- (Just <$> A.takeWhile (not . reserved) <* "@") <|> pure Nothing
    host <- A.takeWhile (not . reserved)
    return SSHSpec{..}

newtype SnapSet = SnapSet {getSnapSet :: Map GUID (Map SnapshotName ObjectMeta)} deriving (Show)

snapshots :: [Object] -> SnapSet
snapshots objs = SnapSet $ Map.fromListWith Map.union [(guidOf meta, Map.singleton name meta) | Snapshot name meta <- objs]


data SendOptions = SendOptions
    { sendCompressedOpt :: Bool
    , sendRawOpt :: Bool
    }

sendOptArgs :: SendOptions -> [String]
sendOptArgs SendOptions{..} = 
    if sendCompressedOpt then ["--compressed"] else [] 
    ++ if sendRawOpt then ["--raw"] else []

data IncrStep = IncrStep {
        startGUIDOf :: GUID,
        startSrcNameOf :: SnapshotName,
        stopGUIDOf :: GUID,
        stopSrcNameOf :: SnapshotName
    }   deriving Show

sendArgs :: SendOptions -> Either SnapshotName IncrStep -> [String]
sendArgs opts send = ["send"] ++ sendOptArgs opts ++ case send of
        Right (IncrStep _ startName _ stopName) -> ["-i", show startName, show stopName]
        Left snap -> [show snap]

recvArgs :: FilesystemName -> Either SnapshotName IncrStep -> [String]
recvArgs dstFS send = 
        let snap =  case send of
                Right (IncrStep _ _ _ (SnapshotName _fs s)) -> s
                Left (SnapshotName _fs s) -> s
        in ["receive", show $ SnapshotName dstFS snap]

data CopyPlan
    = Nada
    | FullCopy GUID SnapshotName FilesystemName
    | Incremental {startDstNameOf :: SnapshotName, incrSteps :: [IncrStep]} deriving Show

sendCommand :: Maybe SSHSpec -> SendOptions -> Either SnapshotName IncrStep -> (String, [String])
sendCommand ssh opts snap = case ssh of
    Nothing -> ("zfs", sendArgs opts snap)
    Just spec -> ("ssh", [show spec, "zfs"] ++ sendArgs opts snap)

recCommand :: Maybe SSHSpec -> FilesystemName -> Either SnapshotName IncrStep -> (String, [String])
recCommand ssh dstFs snap = case ssh of 
    Nothing -> ("zfs", recvArgs dstFs snap)
    Just spec -> ("ssh", [show spec, "zfs"] ++ recvArgs dstFs snap)

formatCommand :: (String, [String]) -> String
formatCommand (cmd, args) = intercalate " " (cmd : args)

showShell :: Maybe SSHSpec -> Maybe SSHSpec -> SendOptions ->  CopyPlan -> String
showShell _ _ _ Nada = "true"
showShell send rcv opts (FullCopy _ snap dstFs) = formatCommand (sendCommand send opts (Left snap)) ++ " | pv | " ++ formatCommand (recCommand rcv dstFs (Left snap))
showShell send rcv opts (Incremental startDstSnap steps) 
    = concat $ map (\step -> 
        formatCommand (sendCommand send opts (Right step)) ++ " | pv | " ++ 
        formatCommand (recCommand rcv (snapshotFSOf startDstSnap) (Right step)) ++ "\n")
        steps

prettyPlan :: CopyPlan -> String
prettyPlan Nada = "Do Nothing"
prettyPlan (FullCopy _ name _) = "Full copy: " ++ show name
prettyPlan (Incremental dstInit steps) = "Incremental copy. Starting from " ++ show dstInit ++ " on dest.\n" ++ concatMap prettyStep steps
    where 
    prettyStep (IncrStep _ startName _ stopName) = show startName ++ " -> " ++ show stopName ++ "\n"

withFS :: FilesystemName -> SnapSet -> SnapSet
withFS fsName (SnapSet snaps) = 
    SnapSet $ Map.filter (not . null) $ Map.map (Map.filterWithKey relevant) snaps
    where
    relevant name _meta = snapshotFSOf name == fsName


presentIn :: SnapSet -> SnapSet -> SnapSet
(SnapSet a) `presentIn` (SnapSet b) = SnapSet (Map.intersection a b)


triplets :: SnapSet -> [(GUID, SnapshotName, UTCTime)]
triplets (SnapSet snaps) = [(guid,name,creationOf meta) | (guid,snaps') <- Map.toList snaps, (name,meta) <- Map.toList snaps']

byDate :: SnapSet -> Map UTCTime (Set (GUID, SnapshotName))
byDate = Map.fromListWith Set.union . map (\(guid,name,time) -> (time,Set.singleton (guid,name))) . triplets


single :: Set (GUID, SnapshotName) -> Either String (GUID, SnapshotName) 
single snaps = case Set.minView snaps of
    Nothing -> Left "Error: Zero available matching snaps. Bug?"
    Just (lowest,others) | null others -> Right lowest
                      | otherwise -> Left $ "Error: Ambiguity between snaps: " ++ show lowest ++ " " ++ show others

copyPlan :: FilesystemName -> SnapSet -> FilesystemName -> SnapSet -> Either String CopyPlan
copyPlan srcFS src dstFS dst = 
    case Map.lookupMax dstByDate of
            Nothing -> case Map.lookupMax srcByDate  of 
                Nothing -> Right Nada -- No backups to copy over!
                Just (_date, srcSnaps) -> do
                    (guid,name) <- single srcSnaps
                    Right (FullCopy guid name dstFS)                    
            Just (latestDstDate, dstSnaps) ->  do
                (latestDstGUID, latestDstName) <- single dstSnaps
                let toCopy = Map.dropWhileAntitone (< latestDstDate) srcByDate 
                    inOrder = Map.elems toCopy
                case inOrder of
                    [] -> return ()
                    (initial:_) -> do
                        (initialGUID,_) <- single initial  
                        when (latestDstGUID /= initialGUID) (Left "Error: Initial sync GUID mismatch")
                -- TODO: Ensure starting GUIDs match (use _latestDstGUID)
                steps <- zipWithM (\as bs -> do
                    (aGUID,aName) <- single as
                    (bGUID,bName) <- single bs
                    Right $ IncrStep aGUID aName bGUID bName)
                    inOrder 
                    (tail inOrder)
                Right $ Incremental latestDstName steps
    where
    relevantOnSrc = withFS srcFS src
    relevantOnDst = withFS dstFS dst `presentIn` relevantOnSrc
    srcByDate = byDate relevantOnSrc
    dstByDate = byDate relevantOnDst




-- data Remotable a = Remotable {
--     ssh :: Maybe SSHSpec,
--     thing :: a
-- } deriving Show

-- remotableP :: A.Parser a -> A.Parser (Remote a)
-- remotableP a = do
--     ssh <- sshSpecP
--     ":"
--     thing <- a
--     return Remote{..}




