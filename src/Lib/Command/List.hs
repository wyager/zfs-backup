module Lib.Command.List (listPrint,list) where
import           Control.Concurrent.STM (STM, atomically)
import qualified Control.Exception      as Ex
import qualified Data.Attoparsec.Text   as A
import           Data.ByteString        (ByteString)
import qualified Data.ByteString.Lazy   as LBS
import qualified Data.Text.Encoding     as TE
import           Lib.Common             (SSHSpec, Should, BeVerbose, UseFreeBSDMode, should)
import           Lib.ZFS                (Object, listShellCmd, listSnapsShellCmd, objects, Object(Snapshot), SnapshotName, FilesystemName, snapshotFSOf)
import           System.Exit            (ExitCode (ExitFailure, ExitSuccess))
import qualified System.Process.Typed   as P
import           Text.Regex.TDFA ((=~))
import           Control.Monad          (when)

data ListError = CommandError ByteString | ZFSListParseError String deriving (Show, Ex.Exception)

listPrint ::  Should BeVerbose -> Maybe SSHSpec -> (SnapshotName sys -> Bool) -> Should UseFreeBSDMode -> IO ()
listPrint verbose host excluding freebsd = list verbose Nothing host excluding freebsd >>= either Ex.throw (mapM_ print)

list :: Should BeVerbose -> Maybe (FilesystemName sys) -> Maybe SSHSpec -> (SnapshotName sys -> Bool) -> Should UseFreeBSDMode -> IO (Either ListError (Maybe [Object sys]))
list verbose fs host excluding freebsd = listWith verbose excluding' $ case host of 
    Nothing -> localCmd freebsd fs
    Just spec -> sshCmd freebsd spec fs
    where
    fsMatches name = case fs of 
        Just filesystem -> snapshotFSOf name == filesystem
        Nothing -> True
    -- If we're using freebsd, we have to filter by fs name ourselves
    excluding' name = 
        if should @UseFreeBSDMode freebsd 
            then excluding name || not (fsMatches name)
            else excluding name

filterWith :: (SnapshotName sys -> Bool) -> [Object sys] -> [Object sys]
filterWith excluding = filter (not . excluded)
    where
    excluded (Snapshot snap _meta) = excluding snap
    excluded _ = False 

listWith :: forall sys . Should BeVerbose -> (SnapshotName sys -> Bool) -> P.ProcessConfig () () () -> IO (Either ListError (Maybe [Object sys]))
listWith verbose excluding cmd = do
    let cmd' = allOutputs cmd 
    when (should @BeVerbose verbose) $ print cmd'
    output <- P.withProcessWait cmd' $ \proc -> do
        output <- fmap (TE.decodeUtf8 . LBS.toStrict) $ atomically $ P.getStdout proc
        err <- fmap LBS.toStrict $ atomically $ P.getStderr proc
        P.waitExitCode proc >>= \case
            ExitSuccess -> return (Right $ Just output)
            ExitFailure _i -> if err =~ ("dataset does not exist" :: ByteString)
                then return $ Right $ Nothing
                else return $ Left $ CommandError err
    let result :: Either ListError (Maybe [Object sys])
        result = case output of 
            Right Nothing -> Right Nothing
            Right (Just outputData) -> case A.parseOnly objects outputData of
                Left err -> Left (ZFSListParseError err)
                Right parseObjects -> Right $ Just $ filterWith excluding parseObjects
            Left err -> Left err
    return result


dontIncludeFsNameOnFreebsd :: Should UseFreeBSDMode -> a -> Maybe a
dontIncludeFsNameOnFreebsd freebsd fsName = if should @UseFreeBSDMode freebsd
    then Just fsName
    else Nothing

localCmd :: Should UseFreeBSDMode -> Maybe (FilesystemName sys) -> P.ProcessConfig () () ()
localCmd freebsd = maybe (P.shell listShellCmd) (P.shell . listSnapsShellCmd . dontIncludeFsNameOnFreebsd freebsd) 

sshCmd :: Should UseFreeBSDMode -> SSHSpec -> Maybe (FilesystemName sys) -> P.ProcessConfig () () ()
sshCmd freebsd spec fs = maybe (shell listShellCmd) (shell . listSnapsShellCmd . dontIncludeFsNameOnFreebsd freebsd) fs
    where
    shell = P.shell . (\cmd -> "ssh " ++ show spec ++ " " ++ cmd)

allOutputs :: P.ProcessConfig () () () -> P.ProcessConfig () (STM LBS.ByteString) (STM LBS.ByteString)
allOutputs command = P.setStdin P.closed $ P.setStdout P.byteStringOutput $ P.setStderr P.byteStringOutput command
