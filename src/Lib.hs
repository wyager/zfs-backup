module Lib (runCommand) where

import Lib.Delete (cleanup)
import Lib.List (list) 
import Lib.Copy (copy)
import GHC.Generics (Generic)
import Options.Generic (ParseRecord, type (:::), type (<?>), Wrapped, 
    parseRecord, parseRecordWithModifiers, lispCaseModifiers, unwrapRecord)


import Lib.Common (Remotable,SSHSpec)
import Lib.ZFS (FilesystemName)
import Lib.Units(History)

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
            


