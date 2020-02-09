module Lib (runCommand) where

import Lib.Delete (cleanup)
import Lib.List (list) 
import Lib.Copy (copy)
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


import Lib.Common (Remotable,SSHSpec,remotable,thing)
import Lib.ZFS (FilesystemName,SnapshotName(..),GUID,SnapSet,snapshots,single,withFS,presentIn,byDate)
import Lib.Units(History)
import Lib.Progress (printProgress)

    -- bin time let (base,frac) = temporalDiv unit time in (base, frac, v)) times

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



runCommand :: IO ()
runCommand = do
    command <- unwrapRecord "ZFS Backup Tool"
    case command of
        List host -> list host >>= print
        CopySnapshots{..} -> copy src dst sendCompressed sendRaw dryRun
        CleanupSnapshots{..} -> cleanup filesystem mostRecent alsoKeep dryRun
            


