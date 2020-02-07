module Lib where

import Data.Word (Word64, Word16)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Char8 as BS
import Data.Time.Clock (UTCTime, NominalDiffTime)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import qualified Data.Attoparsec.ByteString.Char8 as A
import Control.Applicative (many, (<|>))
import qualified System.Process.Typed as P
import System.Exit (ExitCode(ExitSuccess,ExitFailure))
import System.IO (Handle)
import GHC.Conc (STM, atomically)
import GHC.Generics (Generic)
import Options.Generic (ParseRecord, ParseField, readField, getRecord)
import qualified Options.Applicative as Opt

data Error = CommandError ByteString | ZFSListParseError String deriving Show

data Command 
    = List {
        remote :: Maybe SSHSpec
    } 
    | Foo
    deriving (Generic, ParseRecord, Show)


someFunc :: IO ()
someFunc = do
    command :: Command <- getRecord "Tool"
    print command
    -- output <- P.withProcessWait (allOutputs localCmd) $ \proc -> do
    --     bytes <- fmap LBS.toStrict $ atomically $ P.getStdout proc
    --     err <- fmap LBS.toStrict $ atomically $ P.getStderr proc
    --     P.waitExitCode proc >>= \case
    --         ExitSuccess -> return (Right bytes)
    --         ExitFailure _i -> return $ Left $ CommandError err
    -- let result = output >>= either (Left . ZFSListParseError) Right . A.parseOnly objects 
    -- either print (mapM_ print) result

newtype Size = Size Word64 deriving newtype (Eq, Ord, Show, Num)

newtype ObjName = ObjName ByteString deriving newtype (Eq, Ord, Show)

newtype PoolName = PoolName ByteString deriving newtype (Eq, Ord, Show)

newtype GUID = GUID Word64 deriving newtype (Eq, Ord, Show)

data Type = Filesystem | Volume | Snapshot deriving (Eq, Ord, Show)

data Object = Object 
    { type_ :: !Type
    , name :: !ObjName
    , creation :: !UTCTime
    , guid :: !GUID
    , referenced :: !Size
    , used :: !Size
    } deriving Show

object :: A.Parser Object
object = Object <$> type_ <*> t name <*> t creation <*> t guid <*> t size <*> t size <* A.endOfLine
    where
    t x = A.char '\t' *> x
    type_ = filesystem <|> snapshot <|> volume
    filesystem = Filesystem <$ A.string "filesystem"
    snapshot = Snapshot <$ A.string "snapshot"
    volume = Volume <$ A.string "volume"
    name = ObjName <$> A.takeWhile (/= '\t')
    creation = seconds <$> A.decimal
    guid = GUID <$> A.decimal
    size = Size <$> A.decimal

objects :: A.Parser [Object]
objects = many object <* A.endOfInput



seconds :: Word64 -> UTCTime
seconds = posixSecondsToUTCTime . fromIntegral

class PoolM m where 
    contents :: PoolName -> m [Object]

localCmd :: P.ProcessConfig () () ()
localCmd = "zfs list -Hp -t all -o type,name,creation,guid,referenced,used" 

allOutputs :: P.ProcessConfig () () () -> P.ProcessConfig () (STM LBS.ByteString) (STM LBS.ByteString)
allOutputs command = P.setStdin P.closed $ P.setStdout P.byteStringOutput $ P.setStderr P.byteStringOutput command


data SSHSpec = SSHSpec {
    user :: Maybe ByteString,
    host :: ByteString
} deriving (Show, Generic)

instance ParseField SSHSpec where
    readField = Opt.eitherReader (either (Left . ("Parse error: " ++ )) Right . A.parseOnly (sshSpecP <* A.endOfInput) . BS.pack)

sshSpecP :: A.Parser SSHSpec
sshSpecP = do
    let reserved = A.inClass " @/"
    user <- (Just <$> A.takeWhile (not . reserved) <* "@") <|> pure Nothing
    host <- A.takeWhile (not . reserved)
    return SSHSpec{..}


-- data Remote a = Remote {
--     ssh :: SSHSpec,
--     thing :: a
-- } deriving Show

-- remoteP :: A.Parser a -> A.Parser (Remote a)
-- remoteP a = do
--     ssh <- sshSpecP
--     ":"
--     thing <- a
--     return Remote{..}



