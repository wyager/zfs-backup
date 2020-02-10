module Lib.Command.Copy (copy,speedTest) where
import qualified Control.Exception     as Ex
import           Control.Monad         (when, zipWithM)
import qualified Data.ByteString.Char8 as BS
import           Data.List             (intercalate)
import qualified Data.Map.Strict       as Map
import           Lib.Common            (Remotable, SSHSpec, remotable, thing)
import           Lib.Command.List      (list)
import           Lib.Progress          (printProgress)
import           Lib.ZFS               (FilesystemName, GUID, SnapSet,
                                        SnapshotName (..), byDate, presentIn,
                                        single, snapshots, withFS)
import           System.IO             (Handle, hClose)
import qualified System.Process.Typed  as P

copy ::  Remotable FilesystemName ->  Remotable FilesystemName -> Bool -> Bool -> Bool -> IO ()
copy src dst sendCompressed sendRaw dryRun = do
    let srcRemote = remotable Nothing Just src
        dstRemote = remotable Nothing Just dst
    srcSnaps <- either Ex.throw (return . snapshots) =<< list srcRemote
    dstSnaps <- either Ex.throw (return . snapshots) =<< list dstRemote
    case copyPlan (thing src) srcSnaps (thing dst) dstSnaps of
        Left err -> print err
        Right plan -> if dryRun
            then putStrLn (showShell srcRemote dstRemote (SendOptions sendCompressed sendRaw) plan)
            else executeCopyPlan srcRemote dstRemote (SendOptions sendCompressed sendRaw) plan

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



executeCopyPlan :: Maybe SSHSpec -> Maybe SSHSpec -> SendOptions -> CopyPlan -> IO ()
executeCopyPlan sndSpec rcvSpec sndOpts plan = case plan of
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
        printProgress ("Copying to " ++ show rcvSpec) $ \progress -> oneStep progress sndProc rcvProc


data IncrStep = IncrStep {
        startSrcNameOf :: SnapshotName,
        stopSrcNameOf  :: SnapshotName
    }   deriving Show

sendArgs :: SendOptions -> Either SnapshotName IncrStep -> [String]
sendArgs opts send = ["send"] ++ sendOptArgs opts ++ case send of
        Right (IncrStep startName stopName) -> ["-i", show startName, show stopName]
        Left snap -> [show snap]

recvArgs :: FilesystemName -> Either SnapshotName IncrStep -> [String]
recvArgs dstFS send =
        let snap =  case send of
                Right (IncrStep _ (SnapshotName _fs s)) -> s
                Left (SnapshotName _fs s)               -> s
        in ["receive", "-o", "mountpoint=none", show $ SnapshotName dstFS snap]




data CopyPlan
    = CopyNada
    | FullCopy GUID SnapshotName FilesystemName
    | Incremental {_startDstNameOf :: SnapshotName, _incrSteps :: [IncrStep]}

sendCommand :: Maybe SSHSpec -> SendOptions -> Either SnapshotName IncrStep -> (String, [String])
sendCommand ssh opts snap = case ssh of
    Nothing   -> ("zfs", sendArgs opts snap)
    Just spec -> ("ssh", [show spec, "zfs"] ++ sendArgs opts snap)

recCommand :: Maybe SSHSpec -> FilesystemName -> Either SnapshotName IncrStep -> (String, [String])
recCommand ssh dstFs snap = case ssh of
    Nothing   -> ("zfs", recvArgs dstFs snap)
    Just spec -> ("ssh", [show spec, "zfs"] ++ recvArgs dstFs snap)

data SendOptions = SendOptions
    { sendCompressedOpt :: Bool
    , sendRawOpt        :: Bool
    }

sendOptArgs :: SendOptions -> [String]
sendOptArgs SendOptions{..} =
    if sendCompressedOpt then ["--compressed"] else []
    ++ if sendRawOpt then ["--raw"] else []




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

speedTest :: IO ()
speedTest = printProgress "Speed test" $ \update -> do
    let sndProc = P.setStdin P.closed $ P.setStdout P.createPipe $ "dd bs=1m if=/dev/zero count=10000"
    let rcvProc = P.setStdout P.closed $ P.setStdin P.createPipe $ "dd bs=1m of=/dev/null"
    oneStep update sndProc rcvProc

