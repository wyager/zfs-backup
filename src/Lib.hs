module Lib
    ( someFunc
    ) where

import Data.Word (Word64)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString as BS
import Data.Time.Clock (UTCTime, NominalDiffTime)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import qualified Data.Attoparsec.ByteString.Char8 as A
import Control.Applicative (many, (<|>))
import qualified System.Process.Typed as P
import System.Exit (ExitCode(ExitSuccess,ExitFailure))
import System.IO (Handle)
import GHC.Conc (STM, atomically)

someFunc :: IO ()
someFunc = do
    output <- P.withProcessWait runLocal $ \proc -> do
        bytes <- fmap LBS.toStrict $ atomically $ P.getStdout proc
        err <- fmap LBS.toStrict $ atomically $ P.getStderr proc
        P.waitExitCode proc >>= \case
            ExitSuccess -> return (Right bytes)
            ExitFailure i -> return (Left (i,err))
    either print (either print (mapM_ print) . A.parseOnly objects) output

newtype Size = Size Word64 deriving newtype (Eq, Ord, Show, Num)

newtype Name = Name ByteString deriving newtype (Eq, Ord, Show)

newtype PoolName = PoolName ByteString deriving newtype (Eq, Ord, Show)

newtype GUID = GUID Word64 deriving newtype (Eq, Ord, Show)

data Type = Filesystem | Snapshot deriving (Eq, Ord, Show)

data Object = Object 
    { type_ :: !Type
    , name :: !Name
    , creation :: !UTCTime
    , guid :: !GUID
    , referenced :: !Size
    , used :: !Size
    } deriving Show

object :: A.Parser Object
object = Object <$> type_ <*> t name <*> t creation <*> t guid <*> t size <*> t size <* A.endOfLine
    where
    t x = A.char '\t' *> x
    type_ = filesystem <|> snapshot
    filesystem = Filesystem <$ ("filesystem" :: A.Parser ByteString)
    snapshot = Snapshot <$ ("snapshot" :: A.Parser ByteString)
    name = Name <$> A.takeWhile (/= '\t')
    creation = seconds <$> A.decimal
    guid = GUID <$> A.decimal
    size = Size <$> A.decimal

objects :: A.Parser [Object]
objects = many object <* A.endOfInput



seconds :: Word64 -> UTCTime
seconds = posixSecondsToUTCTime . fromIntegral

class PoolM m where 
    contents :: PoolName -> m [Object]

command :: P.ProcessConfig () () ()
command = "zfs list -Hp -t all -o type,name,creation,guid,referenced,used" 

runLocal :: P.ProcessConfig () (STM LBS.ByteString) (STM LBS.ByteString)
runLocal = P.setStdin P.closed $ P.setStdout P.byteStringOutput $ P.setStderr P.byteStringOutput command


-- instance Pool 