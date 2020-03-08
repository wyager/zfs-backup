module Lib.Command.Copy (copy,speedTest,BufferConfig(..),defaultBufferConfig) where
import qualified Control.Exception     as Ex
import           Control.Monad         (when)
import qualified Data.ByteString.Char8 as BS
import           Data.List             (intercalate)
import qualified Data.Map.Strict       as Map
import           Lib.Common            (Remotable, SSHSpec, remotable, thing, Src, Dst,
                                        Should, should, SendCompressed, SendRaw, 
                                        DryRun, OperateRecursively, ForceFullSend)
import           Lib.Command.List      (list)
import           Lib.Progress          (printProgress)
import           Lib.ZFS               (FilesystemName, ObjSet,
                                        SnapshotName (..), SnapshotIdentifier(..), byDate, presentIn,
                                        single, snapshots, withFS)
import           System.IO             (Handle, hClose)
import qualified System.Process.Typed  as P
import           Data.Bifunctor        (first)
import qualified Control.Concurrent.Async as Async
import qualified Control.Concurrent.Chan.Unagi.Bounded as Chan

data BufferConfig = BufferConfig {maxSegs :: Int} deriving Show
defaultBufferConfig :: BufferConfig
defaultBufferConfig = BufferConfig {maxSegs = 16} 

copy :: Remotable (FilesystemName Src) ->  Remotable (FilesystemName Dst) 
     -> Should SendCompressed -> Should SendRaw -> Should DryRun 
     -> (forall sys . SnapshotName sys -> Bool) -> Should OperateRecursively
     -> Should ForceFullSend -> BufferConfig -> IO ()
copy src dst sendCompressed sendRaw dryRun excluding recursive sendFull bufferConfig = do
    let srcRemote = remotable Nothing Just src
        dstRemote = remotable Nothing Just dst
    srcSnaps <- either Ex.throw (return . snapshots) =<< list (Just $ thing src) srcRemote excluding
    dstSnaps <- either Ex.throw (return . snapshots) =<< list (Just $ thing dst) dstRemote excluding
    originalPlan <- either Ex.throw return $ copyPlan (thing src) (withFS (thing src) srcSnaps) (thing dst) (withFS (thing dst) dstSnaps)
    plan <- if should @ForceFullSend sendFull 
                then fullify originalPlan <$ 
                     putStrLn "Unconditionally sending full snapshot. \
                              \This will probably fail, because you'll need remove destination snapshots, \
                              \but that's dangerous so I'm not doing it for you." 
                else return originalPlan
    if should @DryRun dryRun
        then putStrLn (showShell srcRemote dstRemote (SendOptions sendCompressed sendRaw) recursive plan) 
        else executeCopyPlan srcRemote dstRemote (SendOptions sendCompressed sendRaw) plan recursive bufferConfig

oneStep :: BufferConfig -> (Int -> IO ()) -> P.ProcessConfig () Handle () ->  P.ProcessConfig Handle () () -> IO ()
oneStep bufferCfg progress sndProc rcvProc = do
    print sndProc
    print rcvProc
    P.withProcessWait_ rcvProc $ \rcv ->
        P.withProcessWait_ sndProc $ \send -> do
            (writeChan, readChan) <- Chan.newChan (maxSegs bufferCfg)
            writer <- Async.async $ do
                let sndHdl = P.getStdout send
                let go = do
                        chunk <- BS.hGetSome sndHdl 0x20000
                        Chan.writeChan writeChan chunk
                        if BS.null chunk
                            then return ()
                            else go 
                go
            reader <- Async.async $ do
                let rcvHdl = P.getStdin rcv
                let go = do
                        chunk <- Chan.readChan readChan
                        BS.hPut rcvHdl chunk
                        if BS.null chunk
                            then hClose rcvHdl
                            else progress (BS.length chunk) >> go
                go
            ((),()) <- Async.waitBoth reader writer
            return ()

executeCopyPlan :: Maybe SSHSpec -> Maybe SSHSpec -> SendOptions -> CopyPlan -> Should OperateRecursively -> BufferConfig -> IO ()
executeCopyPlan sndSpec rcvSpec sndOpts plan recursive bufferConfig = case plan of
    CopyNada -> putStrLn "Nothing to do"
    FullCopy snap dstFs -> goWith Nothing snap dstFs
    Incremental start stop dstFs -> goWith (Just start) stop dstFs
    where
    goWith start stop dstFs = do
        let (sndExe,sndArgs) = sendCommand sndSpec sndOpts start stop recursive
        let (rcvExe,rcvArgs) = recCommand rcvSpec dstFs
        let sndProc = P.setStdin P.closed $ P.setStdout P.createPipe $ P.proc sndExe sndArgs
        let rcvProc = P.setStdout P.closed $ P.setStdin P.createPipe $ P.proc rcvExe rcvArgs
        printProgress ("Copying to " ++ show dstFs) $ \progress -> oneStep bufferConfig progress sndProc rcvProc

sendArgs :: SendOptions -> Maybe (SnapshotIdentifier Src) -> SnapshotName Src -> Should OperateRecursively -> [String]
sendArgs opts start stop recursively = ["send"] ++ sendOptArgs opts ++ (if should @OperateRecursively recursively then ["-R"] else []) ++ case start of
        Just startFs -> ["-I", show startFs, show stop]
        Nothing -> [show stop]

recvArgs :: FilesystemName Dst -> [String]
recvArgs dstFS = ["receive", "-u", show dstFS]

data CopyPlan
    = CopyNada
    | FullCopy (SnapshotName Src) (FilesystemName Dst)
    | Incremental (SnapshotIdentifier Src) (SnapshotName Src) (FilesystemName Dst)

fullify :: CopyPlan -> CopyPlan
fullify (Incremental _startSnap stopSnap dstFs) = FullCopy stopSnap dstFs
fullify plan = plan

sendCommand :: Maybe SSHSpec -> SendOptions -> Maybe (SnapshotIdentifier Src) -> SnapshotName Src -> Should OperateRecursively -> (String, [String])
sendCommand ssh opts start stop recursively = case ssh of
    Nothing   -> ("zfs", sendArgs opts start stop recursively)
    Just spec -> ("ssh", [show spec, "zfs"] ++ sendArgs opts start stop recursively)

recCommand :: Maybe SSHSpec -> FilesystemName Dst -> (String, [String])
recCommand ssh dstFs = case ssh of
    Nothing   -> ("zfs", recvArgs dstFs)
    Just spec -> ("ssh", [show spec, "zfs"] ++ recvArgs dstFs)

data SendOptions = SendOptions (Should SendCompressed) (Should SendRaw)
    
sendOptArgs :: SendOptions -> [String]
sendOptArgs (SendOptions compressed raw)  =
    if should @SendCompressed compressed then ["--compressed"] else []
    ++ if should @SendRaw raw then ["--raw"] else []


formatCommand :: (String, [String]) -> String
formatCommand (cmd, args) = intercalate " " (cmd : args)

showShell :: Maybe SSHSpec -> Maybe SSHSpec -> SendOptions ->  Should OperateRecursively -> CopyPlan -> String
showShell _ _ _ _ CopyNada = "# nothing to do #"
showShell send rcv opts recursively (FullCopy snap dstFs) = formatCommand (sendCommand send opts Nothing snap recursively) ++ " | pv | " ++ formatCommand (recCommand rcv dstFs)
showShell send rcv opts recursively (Incremental start stop dstFs)
    = formatCommand (sendCommand send opts (Just start) stop recursively) ++ " | pv | " ++
      formatCommand (recCommand rcv dstFs) ++ "\n"
        
prettyPlan :: CopyPlan -> String
prettyPlan CopyNada = "Do Nothing"
prettyPlan (FullCopy name _dstFs) = "Full copy: " ++ show name
prettyPlan (Incremental start stop _dstFs) = "Incremental copy. Starting from " ++ show start ++ " on dest to " ++ show stop

instance Show CopyPlan where
    show = prettyPlan


data CopyError = MustRollback (SnapshotName Dst) | NoSharedSnaps | NoSourceSnaps | AmbiguityError String | DateInconsistency (SnapshotName Src) (SnapshotName Dst) deriving (Ex.Exception)
instance Show CopyError where
    show (MustRollback snap) = 
        let issue = "Error: Most recent snap(s) on destination don't exist on source. "
            help = "Solution: on dest, run: zfs rollback -r " ++ show snap
            notice = " on destination. This will destroy more recent snaps on destination."
        in issue ++ help ++ notice
    show NoSharedSnaps = "There are no snaps that exist on both source and destination"
    show NoSourceSnaps = "Error: Snaphots exist on dest, but not source"
    show (AmbiguityError str) = str
    show (DateInconsistency src dst) = 
        "Error: There is a mismatch in date values. The source thinks " ++ show src 
        ++ " is the latest shared snap, but the dest thinks " ++ show dst
        ++ " is the latest shared snap"



copyPlan :: FilesystemName Src -> ObjSet (SnapshotIdentifier Src) -> FilesystemName Dst -> ObjSet (SnapshotIdentifier Dst) -> Either CopyError CopyPlan
copyPlan srcFS src dstFS dst =
    case Map.lookupMax dstByDate of
            Nothing -> case Map.lookupMax srcByDate  of
                Nothing -> Right CopyNada -- No backups to copy over!
                Just (_date, srcSnaps) -> do
                    (_guid,name) <- first AmbiguityError $ single srcSnaps
                    Right (FullCopy (SnapshotName srcFS name) dstFS)
            Just (_latestDstDate, dstSnaps) ->  do
                (latestDstGUID, _latestDstName) <- first AmbiguityError $ single dstSnaps
                (latestSrcGUID, latestSrcName) <- case Map.lookupMax srcByDate  of
                    Nothing -> Left NoSourceSnaps
                    Just (_date, srcSnaps) -> first AmbiguityError $ single srcSnaps
                let latests = (,) <$> Map.lookupMax (byDate (src `presentIn` dst))
                                  <*> Map.lookupMax (byDate (dst `presentIn` src))
                (latestBothGUID, latestBothSrcName, latestBothDstName) <- case latests of
                    Nothing -> Left NoSharedSnaps
                    Just ((_srcDate, bothSrcSnaps), (_dstDate, bothDstSnaps)) -> do
                        (srcSharedGUID, srcSharedName) <- first AmbiguityError $ single bothSrcSnaps
                        (dstSharedGUID, dstSharedName) <- first AmbiguityError $ single bothDstSnaps
                        when (srcSharedGUID /= dstSharedGUID) $ Left $ 
                            DateInconsistency (SnapshotName srcFS srcSharedName) (SnapshotName dstFS dstSharedName)
                        return (srcSharedGUID, srcSharedName, dstSharedName)
                when (latestDstGUID /= latestBothGUID) $ Left $ MustRollback $ SnapshotName dstFS latestBothDstName 
                if latestDstGUID == latestSrcGUID
                    then Right CopyNada
                    else Right $ Incremental latestBothSrcName (SnapshotName srcFS latestSrcName) dstFS
    where
    srcByDate = byDate src
    dstByDate = byDate dst

speedTest :: IO ()
speedTest = printProgress "Speed test" $ \update -> do
    let sndProc = P.setStdin P.closed $ P.setStdout P.createPipe $ "dd bs=1m if=/dev/zero count=10000"
    let rcvProc = P.setStdout P.closed $ P.setStdin P.createPipe $ "dd bs=1m of=/dev/null"
    oneStep defaultBufferConfig update sndProc rcvProc

