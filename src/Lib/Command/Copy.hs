module Lib.Command.Copy (copy,speedTest,BufferConfig(..),defaultBufferConfig) where
import qualified Control.Exception     as Ex
import           Control.Monad         (when)
import qualified Data.ByteString.Char8 as BS
import           Data.List             (intercalate)
import qualified Data.Map.Strict       as Map
import           Lib.Common            (Remotable, SSHSpec, remotable, thing, yes, Src, Dst,
                                        Should, should, SendCompressed, SendRaw, 
                                        DryRun, OperateRecursively, ForceFullSend, BeVerbose,
                                        UseFreeBSDMode)
import           Lib.Command.List      (list)
import           Lib.Progress          (printProgress)
import           Lib.ZFS               (FilesystemName, ObjSet,
                                        SnapshotName (..), SnapshotIdentifier(..), byDate, presentIn,
                                        single, snapshots, withFS, rehome)
import           System.IO             (Handle, hClose)
import qualified System.Process.Typed  as P
import           Data.Bifunctor        (first)
import qualified Control.Concurrent.Async as Async
import qualified Control.Concurrent.Chan.Unagi.Bounded as Chan

data BufferConfig = BufferConfig {maxSegs :: Int} deriving Show
defaultBufferConfig :: BufferConfig
defaultBufferConfig = BufferConfig {maxSegs = 16} 

copy :: Remotable (FilesystemName Src) ->  Remotable (FilesystemName Dst) 
     -> Should SendCompressed -> Should SendRaw -> Should DryRun -> Should BeVerbose
     -> (forall sys . SnapshotName sys -> Bool) -> Should OperateRecursively
     -> Should ForceFullSend -> BufferConfig -> Maybe BufferConfig -> Should UseFreeBSDMode -> IO ()
copy src dst sendCompressed sendRaw dryRun verbose excluding recursive sendFull bufferConfig remoteBufferCfg freebsd = do
    let srcRemote = remotable Nothing Just src
        dstRemote = remotable Nothing Just dst
    srcSnaps <- either Ex.throw (return . snapshots . maybe [] id) =<< list verbose (Just $ thing src) srcRemote excluding freebsd
    dstSnaps <- either Ex.throw (return . snapshots . maybe [] id) =<< list verbose (Just $ thing dst) dstRemote excluding freebsd
    originalPlan <- either Ex.throw return $ copyPlan (thing src) (withFS (thing src) srcSnaps) (thing dst) (withFS (thing dst) dstSnaps)
    plan <- if should @ForceFullSend sendFull 
                then fullify originalPlan <$ 
                     putStrLn "Unconditionally sending full snapshot. \
                              \This will probably fail, because you'll need remove destination snapshots, \
                              \but that's dangerous so I'm not doing it for you." 
                else return originalPlan
    if should @DryRun dryRun
        then putStrLn (showShell srcRemote dstRemote (SendOptions sendCompressed sendRaw) recursive remoteBufferCfg plan) 
        else executeCopyPlan verbose srcRemote dstRemote (SendOptions sendCompressed sendRaw) plan recursive bufferConfig remoteBufferCfg

newtype Buffered = Buffered Int
instance Show Buffered where
    show (Buffered i) = "Bufferred writes: ~" ++ show i

oneStep :: Should BeVerbose -> BufferConfig -> (Int -> Buffered -> IO ()) -> P.ProcessConfig () Handle () ->  P.ProcessConfig Handle () () -> IO ()
oneStep verbose bufferCfg progress sndProc rcvProc = do
    when (should @BeVerbose verbose) $ print sndProc
    when (should @BeVerbose verbose) $ print rcvProc
    P.withProcessWait_ rcvProc $ \rcv ->
        P.withProcessWait_ sndProc $ \send -> do
            (writeChan, readChan) <- Chan.newChan (maxSegs bufferCfg)
            let getBuffered = Buffered <$> Chan.estimatedLength writeChan
            writer <- Async.async $ do
                let sndHdl = P.getStdout send
                let go = do
                        chunk <- BS.hGetSome sndHdl 0x20000
                        Chan.writeChan writeChan chunk
                        if BS.null chunk
                            then return ()
                            else do
                                progress 0 =<< getBuffered
                                go 
                go
            reader <- Async.async $ do
                let rcvHdl = P.getStdin rcv
                let go = do
                        chunk <- Chan.readChan readChan
                        BS.hPut rcvHdl chunk
                        if BS.null chunk
                            then hClose rcvHdl
                            else do
                                progress (BS.length chunk) =<< getBuffered
                                go
                go
            ((),()) <- Async.waitBoth reader writer
            return ()

executeCopyPlan :: Should BeVerbose -> Maybe SSHSpec -> Maybe SSHSpec -> SendOptions -> CopyPlan -> Should OperateRecursively -> BufferConfig -> Maybe BufferConfig -> IO ()
executeCopyPlan verbose sndSpec rcvSpec sndOpts plan recursive bufferConfig remoteBufferConfig = case plan of
    CopyNada -> putStrLn "Nothing to do"
    FullCopy snap dstFs -> goWith Nothing snap dstFs
    Incremental start stop dstFs -> goWith (Just start) stop dstFs
    where
    goWith start stop dstFs = do
        let (sndExe,sndArgs) = sendCommand sndSpec sndOpts start stop recursive
        let (rcvExe,rcvArgs) = case start of
                Nothing -> recCommand rcvSpec (stop `sameSnapshotOn` dstFs) remoteBufferConfig
                Just _startSnap -> recCommand rcvSpec dstFs remoteBufferConfig -- No need to specify the receiving snapshot name, since we're doing an incremental send
        let sndProc = P.setStdin P.closed $ P.setStdout P.createPipe $ P.proc sndExe sndArgs
        let rcvProc = P.setStdout P.closed $ P.setStdin P.createPipe $ P.proc rcvExe rcvArgs
        printProgress ("Copying to " ++ show dstFs) (Buffered 0) $ \progress -> oneStep verbose bufferConfig progress sndProc rcvProc

sendArgs :: SendOptions -> Maybe (SnapshotIdentifier Src) -> SnapshotName Src -> Should OperateRecursively -> [String]
sendArgs opts start stop recursively = ["send"] ++ sendOptArgs opts ++ (if should @OperateRecursively recursively then ["-R"] else []) ++ case start of
        Just startFs -> ["-I", show startFs, show stop]
        Nothing -> [show stop]


data CopyPlan
    = CopyNada
    | FullCopy (SnapshotName Src) (FilesystemName Dst)
    | Incremental (SnapshotIdentifier Src) (SnapshotName Src) (FilesystemName Dst)

sameSnapshotOn :: SnapshotName Src -> FilesystemName Dst -> SnapshotName Dst
srcSnap `sameSnapshotOn` dstFs = SnapshotName dstFs (rehome $ snapshotNameOf srcSnap)

fullify :: CopyPlan -> CopyPlan
fullify (Incremental _startSnap stopSnap dstFs) = FullCopy stopSnap dstFs
fullify plan = plan

sendCommand :: Maybe SSHSpec -> SendOptions -> Maybe (SnapshotIdentifier Src) -> SnapshotName Src -> Should OperateRecursively -> (String, [String])
sendCommand ssh opts start stop recursively = case ssh of
    Nothing   -> ("zfs", sendArgs opts start stop recursively)
    Just spec -> ("ssh", [show spec, "zfs"] ++ sendArgs opts start stop recursively)

recCommand :: Show (target Dst) => Maybe SSHSpec -> target Dst -> Maybe BufferConfig -> (String, [String])
recCommand ssh target remoteBufferConfig = case ssh of
    Nothing   -> ("zfs", ["receive", "-u", show target])
    Just spec -> case remoteBufferConfig of 
        Nothing -> ("ssh", [show spec, "zfs", "receive", "-u", show target])
        Just (BufferConfig size) -> ("ssh", [show spec, "zfs-backup", "receive", "--receive-to", show target, "--transfer-buffer-count", show size])

data SendOptions = SendOptions (Should SendCompressed) (Should SendRaw)
    
sendOptArgs :: SendOptions -> [String]
sendOptArgs (SendOptions compressed raw)  =
    if should @SendCompressed compressed then ["--compressed"] else []
    ++ if should @SendRaw raw then ["--raw"] else []


formatCommand :: (String, [String]) -> String
formatCommand (cmd, args) = intercalate " " (cmd : args)

showShell :: Maybe SSHSpec -> Maybe SSHSpec -> SendOptions ->  Should OperateRecursively -> Maybe BufferConfig -> CopyPlan -> String
showShell _ _ _ _ _ CopyNada = "# nothing to do #"
showShell send rcv opts recursively remoteBufferConfig (FullCopy snap dstFs) = formatCommand (sendCommand send opts Nothing snap recursively) ++ " | pv | " ++ formatCommand (recCommand rcv (snap `sameSnapshotOn` dstFs) remoteBufferConfig)
showShell send rcv opts recursively remoteBufferConfig (Incremental start stop dstFs)
    = formatCommand (sendCommand send opts (Just start) stop recursively) ++ " | pv | " ++
      formatCommand (recCommand rcv dstFs remoteBufferConfig) ++ "\n"
        
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
speedTest = printProgress "Speed test" (Buffered 0) $ \update -> do
    let sndProc = P.setStdin P.closed $ P.setStdout P.createPipe $ "dd bs=1m if=/dev/zero count=10000"
    let rcvProc = P.setStdin P.createPipe $ "dd bs=1m of=/dev/null"
    oneStep (yes @BeVerbose) defaultBufferConfig update sndProc rcvProc

