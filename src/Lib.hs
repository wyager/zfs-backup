module Lib (speedTest, runCommand, History, HasParser, parser, temporalDiv, TimeUnit(..)) where

import Data.Word (Word64)
import Data.ByteString (ByteString)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Char8 as BS
import Data.Time.Clock (UTCTime(UTCTime))
import qualified Data.Time.Clock as Clock 
import Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import qualified Data.Time.Calendar as Cal
import qualified Data.Attoparsec.Text as A
import Control.Applicative (many, (<|>))
import qualified System.Process.Typed as P
import System.Exit (ExitCode(ExitSuccess,ExitFailure))
import System.IO (Handle, hClose)
import Control.Concurrent.STM (STM, atomically)
import Control.Concurrent (threadDelay)
import GHC.Generics (Generic)
import Options.Generic (ParseRecord, ParseField, ParseFields, 
    Only(fromOnly), Wrapped, readField, unwrapRecord, parseRecord, 
    type (<?>), type (:::), parseRecordWithModifiers, lispCaseModifiers)
import qualified Options.Applicative as Opt
import Data.Bifunctor (first, second)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Control.Exception as Ex
import Control.Monad (zipWithM, when, unless)
import Data.List (intercalate)
import qualified Data.IORef as IORef
import qualified Control.Concurrent.Async as Async
import Text.Printf (printf)
import qualified Net.IPv4 as IP4
import qualified Net.IPv6 as IP6
import Data.Typeable (Typeable)
import Data.Ratio ((%))

data ListError = CommandError ByteString | ZFSListParseError String deriving (Show, Ex.Exception)

data DeleteError = Couldn'tPlan String deriving (Show, Ex.Exception)
    -- bin time let (base,frac) = temporalDiv unit time in (base, frac, v)) times


instance Show TimeUnit where
    show Day = "day"
    show Month = "month"
    show Year = "year"

instance HasParser TimeUnit where
    parser = Day <$ "day" 
         <|> Month <$ "month" 
         <|> Year <$ ("year" <|> "yr")
data Period = Period Integer TimeUnit
instance Show Period where
    show (Period count unit) = show count ++ "-per-" ++ show unit

instance HasParser Period where
    parser = Period <$> A.decimal <*> ("-per-" *> parser)

data History = History Int Period 

instance Show History where
    show (History count period) = show count ++ "@" ++ show period

instance HasParser History where
    parser = History <$> A.decimal <*> ("@" *> parser)

instance ParseField History where
    readField = unWithParser <$> readField


-- Don't worry about the w, (:::), <?> stuff. That's just
-- there to let the arg parser auto-generate docs
data Command w 
    = List {
        remote :: w ::: Maybe SSHSpec <?> "Remote host to list on"
    } 
    | CopySnapshots {
        src :: w ::: Remotable FilesystemName <?> "Can be \"tank/set\" or \"user@host:tank/set\"",
        dst :: w ::: Remotable FilesystemName <?> "Can be \"tank/set\" or \"user@host:tank/set\"",
        sendCompressed :: w ::: Bool <?> "Send using LZ4 compression",
        sendRaw :: w ::: Bool <?> "Send Raw (can be used to securely backup encrypted datasets)",
        dryRun :: w ::: Bool <?> "Don't actually do anything, just print what's going to happen"
    }
    | CleanupSnapshots {
        filesystem :: w ::: Remotable FilesystemName <?> "Can be \"tank/set\" or \"user@host:tank/set\"",
        mostRecent :: w ::: Maybe Int <?> "Keep most recent N snapshots",
        alsoKeep :: w ::: [History] <?> "To keep 1 snapshot per month for the last 12 months, use \"12@1-per-month\". To keep up to 10 snapshots a day, for the last 10 days, use \"100@10-per-day\", and so on. Can use day, month, year. Multiple of these flags will result in all the specified snaps being kept. This all works in UTC time, by the way. I'm not dealing with time zones.",
        dryRun :: w ::: Bool <?> "Don't actually do anything, just print what's going to happen"
    }
    deriving (Generic)

-- CopySnapshots gets translated to copy-snapshots
instance ParseRecord (Command Wrapped) where
    parseRecord = parseRecordWithModifiers lispCaseModifiers

speedTest :: IO ()
speedTest = printProgress $ \update -> do
    let sndProc = P.setStdin P.closed $ P.setStdout P.createPipe $ "dd bs=1m if=/dev/zero count=10000"
    let rcvProc = P.setStdout P.closed $ P.setStdin P.createPipe $ "dd bs=1m of=/dev/null"
    oneStep update sndProc rcvProc


runCommand :: IO ()
runCommand = do
    command <- unwrapRecord "ZFS Backup Tool"
    case command of
        List host -> do
            result <- listWith $ case host of
                    Nothing -> localCmd
                    Just spec -> sshCmd spec
            print result
        CopySnapshots{..} -> do
            let srcList = remotable localCmd sshCmd src
                dstList = remotable localCmd sshCmd dst
                srcRemote = remotable Nothing Just src
                dstRemote = remotable Nothing Just dst
            srcSnaps <- either Ex.throw (return . snapshots) =<< listWith srcList
            dstSnaps <- either Ex.throw (return . snapshots) =<< listWith dstList
            case copyPlan (thing src) srcSnaps (thing dst) dstSnaps of
                Left err -> print err
                Right plan -> if dryRun
                    then putStrLn (showShell srcRemote dstRemote (SendOptions sendCompressed sendRaw) plan)
                    else printProgress $ \update -> executeCopyPlan update srcRemote dstRemote (SendOptions sendCompressed sendRaw) plan
        CleanupSnapshots{..} -> do
            let list = remotable localCmd sshCmd filesystem
                remote = remotable Nothing Just filesystem
            snaps <- either Ex.throw (return . snapshots) =<< listWith list
            plan <- either (Ex.throw . Couldn'tPlan) return $ planDeletion (thing filesystem) snaps (maybe 0 id mostRecent) alsoKeep
            putStrLn $ prettyDeletePlan plan
            unless dryRun $ executeDeletePlan remote plan

-- planDeletion :: FilesystemName -> SnapSet -> Int -> [History] -> Either String DeletePlan

trackProgress :: Int -> (Int -> IO ()) -> ((Int -> IO ()) -> IO a) -> IO a
trackProgress delay report go = do
    counter <- IORef.newIORef 0
    let update i = IORef.atomicModifyIORef' counter (\c -> (c + i, ()))
    done <- Async.async (go update)
    let loop = do
            timer <- Async.async $ threadDelay delay
            Async.waitEither done timer >>= \case
                Left result -> return result
                Right () -> do
                    progress <- IORef.atomicModifyIORef' counter (\c -> (0, c))
                    report progress
                    loop
    loop


sizeWithUnits :: Integral i => String -> i -> String
sizeWithUnits unit i = printf "%.1f %s%s" scaled prefix unit
    where
    f = fromIntegral i :: Double 
    thousands = floor (log f / log 1000) :: Word
    (prefix :: String, divisor :: Double) = case thousands of
        0 -> ("",  1e0) 
        1 -> ("k", 1e3)
        2 -> ("M", 1e6)
        3 -> ("G", 1e9)
        _ -> ("T", 1e12)
    scaled = f / divisor



printProgress :: ((Int -> IO ()) -> IO a) -> IO a
printProgress = trackProgress (1000*1000) (putStrLn . sizeWithUnits "B/sec")


-- About 3 GB/sec on my mbp
oneStep ::  (Int -> IO ()) -> P.ProcessConfig () Handle () ->  P.ProcessConfig Handle () () -> IO ()
oneStep progress sndProc rcvProc = do
    print sndProc
    print rcvProc
    P.withProcessWait_ rcvProc $ \rcv ->
        P.withProcessWait_ sndProc $ \send -> do
            let sndHdl = P.getStdout send
            let rcvHdl = P.getStdin rcv
            let go = do
                    -- The actual fastest on my mbp seems to be hGet 0x10000, 
                    -- but that feels very machine-dependent. Hopefully hGetSome
                    -- with a bit of room will reliably capture most of the max
                    -- possible performance. With this, I get around 3GB/sec
                    chunk <- BS.hGetSome sndHdl 0x20000 
                    BS.hPut rcvHdl chunk
                    if BS.null chunk
                        then hClose rcvHdl
                        else progress (BS.length chunk) >> go
            go

executeCopyPlan :: (Int -> IO ()) -> Maybe SSHSpec -> Maybe SSHSpec -> SendOptions -> CopyPlan -> IO ()
executeCopyPlan progress sndSpec rcvSpec sndOpts plan = case plan of
    CopyNada -> putStrLn "Nothing to do"
    FullCopy _guid snap dstFs -> goWith (Left snap) dstFs
    Incremental start steps -> flip mapM_ steps $ \step ->
        goWith (Right step) (snapshotFSOf start)
    where
    goWith step dstFs = do
        let (sndExe,sndArgs) = sendCommand sndSpec sndOpts step
        let (rcvExe,rcvArgs) = recCommand rcvSpec dstFs step
        let sndProc = P.setStdin P.closed $ P.setStdout P.createPipe $ P.proc sndExe sndArgs
        let rcvProc = P.setStdout P.closed $ P.setStdin P.createPipe $ P.proc rcvExe rcvArgs
        oneStep progress sndProc rcvProc


executeDeletePlan :: Maybe SSHSpec -> DeletePlan -> IO ()
executeDeletePlan delSpec DeletePlan{..} = flip mapM_ toDelete $ \snapshot -> do
    let (delExe, delArgs) = deleteCommand delSpec snapshot
    let delProc = P.setStdin P.closed $ P.proc delExe delArgs
    print delProc
    P.withProcessWait_ delProc $ \_del -> return ()

listWith :: P.ProcessConfig () () () -> IO (Either ListError [Object])
listWith cmd = do
    output <- P.withProcessWait (allOutputs cmd) $ \proc -> do
        output <- fmap (TE.decodeUtf8 . LBS.toStrict) $ atomically $ P.getStdout proc
        err <- fmap LBS.toStrict $ atomically $ P.getStderr proc
        P.waitExitCode proc >>= \case
            ExitSuccess -> return (Right output)
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

class HasParser a where
    parser :: A.Parser a
newtype WithParser a = WithParser {unWithParser :: a}
instance (Typeable a, HasParser a) => ParseField (WithParser a) where
    readField = Opt.eitherReader (first ("Parse error: " ++ ) . A.parseOnly ((WithParser <$> parser) <* A.endOfInput) . T.pack)


newtype FilesystemName = FilesystemName Text 
    deriving stock (Eq, Ord, Generic)
    deriving anyclass ParseRecord


instance HasParser FilesystemName where
    parser = FilesystemName <$> A.takeWhile (not . A.inClass " @\t")


instance Show FilesystemName where
    show (FilesystemName name) = T.unpack name

instance ParseField FilesystemName where
    readField = unWithParser <$> readField
deriving anyclass instance ParseFields FilesystemName

data SnapshotName = SnapshotName {snapshotFSOf :: FilesystemName, snapshotNameOf :: Text} deriving (Eq,Ord)
instance Show SnapshotName where
    show (SnapshotName fs snap) = show fs ++ "@" ++ T.unpack snap

instance HasParser SnapshotName where
    parser = do
        snapshotFSOf <- parser
        snapshotNameOf <- "@" *> A.takeWhile (not . A.inClass " \t")
        return SnapshotName{..}

instance ParseField SnapshotName where
    readField = unWithParser <$> readField


object :: A.Parser Object
object = (fs <|> vol <|> snap) <* A.endOfLine
    where
    fs = "filesystem" *> (Filesystem <$> t parser <*> t meta)
    vol = "volume" *> (Volume <$ A.takeTill A.isEndOfLine)
    snap = "snapshot" *> (Snapshot <$> t parser <*> t meta)
    meta = ObjectMeta <$> creation <*> t guid <*> t size <*> t size
    t x = A.char '\t' *> x
    creation = seconds <$> A.decimal
    guid = GUID <$> A.decimal
    size = Size <$> A.decimal

objects :: A.Parser [Object]
objects = many object <* A.endOfInput



seconds :: Word64 -> UTCTime
seconds = posixSecondsToUTCTime . fromIntegral

shellCmd :: String
shellCmd = "zfs list -Hp -t all -o type,name,creation,guid,referenced,used" 

localCmd :: P.ProcessConfig () () ()
localCmd = P.shell shellCmd

sshCmd :: SSHSpec -> P.ProcessConfig () () ()
sshCmd spec = P.shell $ "ssh " ++ show spec ++ " " ++ shellCmd

allOutputs :: P.ProcessConfig () () () -> P.ProcessConfig () (STM LBS.ByteString) (STM LBS.ByteString)
allOutputs command = P.setStdin P.closed $ P.setStdout P.byteStringOutput $ P.setStderr P.byteStringOutput command

data Host = IPv6Host IP6.IPv6 | IPv4Host IP4.IPv4 | TextHost Text

instance HasParser Host where
    parser = (IPv6Host <$> IP6.parser) 
         <|> (IPv4Host <$> IP4.parser) 
         <|> (TextHost <$> A.takeWhile (not . A.inClass " @:\t"))

instance Show Host where
    show (IPv6Host ip) = T.unpack $ IP6.encode ip
    show (IPv4Host ip) = T.unpack $ IP4.encode ip
    show (TextHost h)  = T.unpack h

data SSHSpec = SSHSpec {
    user :: Maybe Text,
    host :: Host
} deriving (Generic)

instance Show SSHSpec where
    show SSHSpec{..} = case user of
        Nothing -> show host
        Just usr -> T.unpack usr ++ "@" ++ show host

instance ParseField SSHSpec where
    readField = unWithParser <$> readField

instance HasParser SSHSpec where
    parser = do
        let reserved = A.inClass " @/"
        user <- (Just <$> A.takeWhile (not . reserved) <* "@") <|> pure Nothing
        host <- parser
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
        startSrcNameOf :: SnapshotName,
        stopSrcNameOf :: SnapshotName
    }   deriving Show

sendArgs :: SendOptions -> Either SnapshotName IncrStep -> [String]
sendArgs opts send = ["send"] ++ sendOptArgs opts ++ case send of
        Right (IncrStep startName stopName) -> ["-i", show startName, show stopName]
        Left snap -> [show snap]

recvArgs :: FilesystemName -> Either SnapshotName IncrStep -> [String]
recvArgs dstFS send = 
        let snap =  case send of
                Right (IncrStep _ (SnapshotName _fs s)) -> s
                Left (SnapshotName _fs s) -> s
        in ["receive", show $ SnapshotName dstFS snap]

data DeletePlan = DeletePlan {toDelete :: Set SnapshotName, toKeep :: Set SnapshotName} deriving Show

prettyDeletePlan :: DeletePlan -> String
prettyDeletePlan (DeletePlan delete keep) = concat
    [ printf "Deleting %i snaps\n" (length delete)
    , printf "Keeping %i snaps:\n" (length keep)
    , concatMap (printf "  %s\n" . show) (Set.toList keep)
    ]


planDeletion :: FilesystemName -> SnapSet -> Int -> [History] -> Either String DeletePlan
planDeletion fsName snapSet mostRecentN histories = do
    inOrder :: Map UTCTime SnapshotName <- mapM (second snd . single) $ byDate $ withFS fsName snapSet
    let mostRecent = Set.fromList $ map snd $ take mostRecentN $ Map.toDescList inOrder
    let keepHistories = Set.unions $ map (keepHistory inOrder) histories
    let toKeep = Set.union keepHistories mostRecent
    let toDelete = (Set.fromList $ Map.elems inOrder) `Set.difference` toKeep
    return (DeletePlan toDelete toKeep)



data TimeUnit = Day | Month | Year deriving (Eq,Ord)



-- nominalDiff :: TimeUnit -> NominalDiffTime
-- nominalDiff = 

clipTo :: TimeUnit -> UTCTime -> (UTCTime,UTCTime)
clipTo unit time = case unit of
    Day -> let baseline = UTCTime (Clock.utctDay time) 0 in (baseline, Clock.addUTCTime Clock.nominalDay baseline)
    Month -> let (y,m,_d) = Cal.toGregorian (Clock.utctDay time) 
                 baseline = Cal.fromGregorian y m 1
                 end = Cal.fromGregorian y m $ Cal.gregorianMonthLength y m
             in (UTCTime baseline 0, Clock.addUTCTime Clock.nominalDay (UTCTime end 0))
    Year -> let (y,_m,_d) = Cal.toGregorian (Clock.utctDay time) 
                baseline = Cal.fromGregorian y 1 1
                end = Cal.fromGregorian y 12 31
            in (UTCTime baseline 0, Clock.addUTCTime Clock.nominalDay (UTCTime end 0))

temporalDiv :: TimeUnit -> UTCTime -> (UTCTime, Rational)
temporalDiv unit time = (lo, toRational fraction)
    where
    (lo,hi) = clipTo unit time
    elapsed = time `Clock.diffUTCTime` lo
    maxDur = hi `Clock.diffUTCTime` lo
    fraction = elapsed / maxDur

bin :: Integer -> [(Rational, v)] -> Map Rational (Map Rational v)
bin bins = foldl insert initial
    where
    initial = Map.fromList [(i % bins,Map.empty) | i <- [0..bins-1]]
    insert acc (rat,v) = 
        let (lo,at,_) = Map.splitLookup rat acc in
        let key = maybe (error $ "Invalid rational key: " ++ show rat) id $ ((rat <$ at) <|> (fmap (fst . fst) $ Map.maxViewWithKey lo)) in
        Map.adjust (Map.insert rat v) key acc

binBy :: forall v . TimeUnit -> Integer -> [(UTCTime, v)] -> Map UTCTime (Map Rational (Map Rational v))
binBy unit bins times = fmap (bin bins) based
    where
    based :: Map UTCTime [(Rational, v)]
    based = foldl insert Map.empty times
    insert acc (time,v) = 
        let (base,frac) = temporalDiv unit time in 
        let alter = \case
                Nothing -> Just [(frac,v)]
                Just others -> Just $ (frac,v):others
        in
        Map.alter alter base acc


keepHistory :: forall v . (Ord v) => Map UTCTime v -> History -> Set v
keepHistory snaps (History count (Period frac timeUnit)) = Set.fromList $ take count best
    where 
    -- Outer map - "Base time" (e.g. day boundaries if time unit is day)
    -- Middle map - Start of each bin
    -- Inner map - actual time
    binned :: Map UTCTime (Map Rational (Map Rational v))
    binned = binBy timeUnit frac $ Map.toList snaps
    best :: [v]
    best = do
        (_base, perBase) <- Map.toDescList binned
        (_frac, perFrac) <- Map.toDescList perBase
        case Map.maxView perFrac of
            Nothing -> []
            Just (v,_) -> [v]

deleteCommand :: Maybe SSHSpec -> SnapshotName -> (String, [String])
deleteCommand ssh snap = case ssh of
    Nothing -> ("zfs", ["destroy", show snap])
    Just spec -> ("ssh", [show spec, "zfs", "destroy", show snap])

data CopyPlan
    = CopyNada
    | FullCopy GUID SnapshotName FilesystemName
    | Incremental {_startDstNameOf :: SnapshotName, _incrSteps :: [IncrStep]} 

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
showShell _ _ _ CopyNada = "true"
showShell send rcv opts (FullCopy _ snap dstFs) = formatCommand (sendCommand send opts (Left snap)) ++ " | pv | " ++ formatCommand (recCommand rcv dstFs (Left snap))
showShell send rcv opts (Incremental startDstSnap steps) 
    = concat $ map (\step -> 
        formatCommand (sendCommand send opts (Right step)) ++ " | pv | " ++ 
        formatCommand (recCommand rcv (snapshotFSOf startDstSnap) (Right step)) ++ "\n")
        steps

prettyPlan :: CopyPlan -> String
prettyPlan CopyNada = "Do Nothing"
prettyPlan (FullCopy _ name _) = "Full copy: " ++ show name
prettyPlan (Incremental dstInit steps) = "Incremental copy. Starting from " ++ show dstInit ++ " on dest.\n" ++ concatMap prettyStep steps
    where 
    prettyStep (IncrStep startName stopName) = show startName ++ " -> " ++ show stopName ++ "\n"
instance Show CopyPlan where
    show = prettyPlan

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
                Nothing -> Right CopyNada -- No backups to copy over!
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
                    (_aGUID,aName) <- single as
                    (_bGUID,bName) <- single bs
                    Right $ IncrStep aName bName)
                    inOrder 
                    (tail inOrder)
                Right $ Incremental latestDstName steps
    where
    relevantOnSrc = withFS srcFS src
    relevantOnDst = withFS dstFS dst `presentIn` relevantOnSrc
    srcByDate = byDate relevantOnSrc
    dstByDate = byDate relevantOnDst


data Remotable a 
    = Remote SSHSpec a
    | Local a
    deriving Generic

instance (HasParser a, Typeable a) => ParseRecord (Remotable a) where
    parseRecord = fromOnly <$> parseRecord

thing :: Remotable a -> a
thing (Remote _ a) = a
thing (Local a) = a

remotable :: a -> (SSHSpec -> a) -> Remotable x -> a
remotable _ f (Remote spec _) = f spec
remotable def _ (Local _) = def

instance Show a => Show (Remotable a) where
    show (Local a) = show a
    show (Remote spec a) = show spec ++ ":" ++ show a

instance HasParser a => HasParser (Remotable a) where
    parser = remote <|> local
        where
        remote = Remote <$> (parser <* ":") <*> parser
        local = Local <$> parser

instance (HasParser a, Typeable a) => ParseField (Remotable a) where
    readField = unWithParser <$> readField
instance (HasParser a, Typeable a) => ParseFields (Remotable a)


