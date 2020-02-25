module Lib.Command.List (listPrint,list) where
import           Control.Concurrent.STM (STM, atomically)
import qualified Control.Exception      as Ex
import qualified Data.Attoparsec.Text   as A
import           Data.Bifunctor         (first, second)
import           Data.ByteString        (ByteString)
import qualified Data.ByteString.Lazy   as LBS
import qualified Data.Text.Encoding     as TE
import           Lib.Common             (SSHSpec)
import           Lib.ZFS                (Object, listShellCmd, listSnapsShellCmd, objects, Object(Snapshot), SnapshotName, FilesystemName)
import           System.Exit            (ExitCode (ExitFailure, ExitSuccess))
import qualified System.Process.Typed   as P

data ListError = CommandError ByteString | ZFSListParseError String deriving (Show, Ex.Exception)

listPrint :: Maybe SSHSpec -> (SnapshotName sys -> Bool) -> IO ()
listPrint host excluding = list Nothing host excluding >>= either Ex.throw (mapM_ print)

list :: Maybe (FilesystemName sys) -> Maybe SSHSpec -> (SnapshotName sys -> Bool) -> IO (Either ListError [Object sys])
list fs host excluding = listWith excluding $ case host of 
    Nothing -> localCmd fs
    Just spec -> sshCmd spec fs

filterWith :: (SnapshotName sys -> Bool) -> [Object sys] -> [Object sys]
filterWith excluding = filter (not . excluded)
    where
    excluded (Snapshot snap _meta) = excluding snap
    excluded _ = False 

listWith :: (SnapshotName sys -> Bool) -> P.ProcessConfig () () () -> IO (Either ListError [Object sys])
listWith excluding cmd = do
    output <- P.withProcessWait (allOutputs cmd) $ \proc -> do
        output <- fmap (TE.decodeUtf8 . LBS.toStrict) $ atomically $ P.getStdout proc
        err <- fmap LBS.toStrict $ atomically $ P.getStderr proc
        P.waitExitCode proc >>= \case
            ExitSuccess -> return (Right output)
            ExitFailure _i -> return $ Left $ CommandError err
    return $ output >>= second (filterWith excluding) . first ZFSListParseError . A.parseOnly objects



localCmd :: Maybe (FilesystemName sys) -> P.ProcessConfig () () ()
localCmd = maybe (P.shell listShellCmd) (P.shell . listSnapsShellCmd) 

sshCmd :: SSHSpec -> Maybe (FilesystemName sys) -> P.ProcessConfig () () ()
sshCmd spec fs = maybe (shell listShellCmd) (shell . listSnapsShellCmd) fs
    where
    shell = P.shell . (\cmd -> "ssh " ++ show spec ++ " " ++ cmd)

allOutputs :: P.ProcessConfig () () () -> P.ProcessConfig () (STM LBS.ByteString) (STM LBS.ByteString)
allOutputs command = P.setStdin P.closed $ P.setStdout P.byteStringOutput $ P.setStderr P.byteStringOutput command
