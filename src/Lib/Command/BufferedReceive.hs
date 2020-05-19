module Lib.Command.BufferedReceive (receive) where
import qualified Data.ByteString.Char8 as BS
import           Lib.Common            (Dst, Should, BeVerbose, should)
import           Lib.Command.Copy      (BufferConfig(..))
import           Lib.ZFS               (SnapshotOrFilesystemName)
import           System.IO             (Handle, hClose, stdin)
import qualified System.Process.Typed  as P
import qualified Control.Concurrent.Async as Async
import qualified Control.Concurrent.Chan.Unagi.Bounded as Chan
import           Control.Concurrent (threadDelay)
import           Control.Monad (when)



receive :: Should BeVerbose -> SnapshotOrFilesystemName Dst -> BufferConfig -> IO ()
receive verbose dst bufferConfig = do
    let (rcvExe,rcvArgs) = recCommand dst
    let rcvProc = P.setStdout P.closed $ P.setStdin P.createPipe $ P.proc rcvExe rcvArgs
    when (should @BeVerbose verbose) $ print rcvProc
    run bufferConfig rcvProc


run :: BufferConfig -> P.ProcessConfig Handle () () -> IO ()
run bufferCfg rcvProc = do
    P.withProcessWait_ rcvProc $ \rcv -> do
        (writeChan, readChan) <- Chan.newChan (maxSegs bufferCfg)
        let poll = do
                threadDelay (1000 * 1000)
                buffered <- Chan.estimatedLength writeChan
                putStrLn ("Buffered reads on remote end: ~" ++ show buffered)
        Async.withAsync poll $ \_poll -> do
            writer <- Async.async $ do
                let go = do
                        chunk <- BS.hGet stdin 0x10000 -- 65536
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
                            else go
                go
            ((),()) <- Async.waitBoth reader writer
            return ()

recCommand :: Show (target Dst) => target Dst -> (String, [String])
recCommand target = ("zfs", ["receive", "-u", show target])
