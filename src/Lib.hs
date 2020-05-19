module Lib (runCommand) where

import           GHC.Generics    (Generic)
import           Lib.Command.Copy        (copy,BufferConfig(..),defaultBufferConfig)
import           Lib.Command.Delete      (cleanup)
import           Lib.Command.List        (listPrint)
import           Lib.Command.BufferedReceive (receive) 
import           Options.Generic (ParseRecord, Wrapped, 
                                  lispCaseModifiers, unwrapRecord,
                                  parseRecord, parseRecordWithModifiers,
                                  type (:::), type (<?>))
import           Lib.Common      (Remotable, SSHSpec, Src, Dst, 
                                  Should, SendCompressed, SendRaw, DryRun, OperateRecursively, ForceFullSend, BeVerbose)
import           Lib.Units       (History)
import           Lib.ZFS         (FilesystemName, SnapshotName(..), SnapshotOrFilesystemName, identifierOf)
import           Lib.Regex       (Regex, matches)

-- The w, (:::), <?> stuff is just
-- there to let the arg parser auto-generate docs
data Command w
    = List {
        remote :: w ::: Maybe SSHSpec <?> "Remote host to list on",
        ignoring :: w ::: [Regex] <?> "Ignore snapshots with names matching any of these regexes",
        verbose :: w ::: Should BeVerbose <?> "Verbose mode. Prints shell commands."
    }
    | CopySnapshots {
        src :: w ::: Remotable (FilesystemName Src) <?> "Can be \"tank/set\" or \"user@host:tank/set\"",
        dst :: w ::: Remotable (FilesystemName Dst) <?> "Can be \"tank/set\" or \"user@host:tank/set\"",
        sendCompressed :: w ::: Should SendCompressed <?> "Send using LZ4 compression",
        sendRaw :: w ::: Should SendRaw <?> "Send Raw (can be used to securely backup encrypted datasets)",
        dryRun :: w ::: Should DryRun <?> "Don't actually do anything, just print what's going to happen",
        ignoring :: w ::: [Regex] <?> "Ignore snapshots with names matching any of these regexes",
        recursive :: w ::: Should OperateRecursively <?> "Recursive mode. Corresponds to `zfs send -R`, `zfs snapshot -r`, `zfs destroy -r`",
        forceFullSend :: w ::: Should ForceFullSend <?> "Send a full snapshot, even if an incremental snapshot could be sent",
        transferBufferCount :: w ::: Maybe Int <?> "How many reads/writes to buffer between `zfs send` and `zfs receive`. Useful for bursty sends. Default 16",
        remoteBufferCount :: w ::: Maybe Int <?> "How many 2^16-byte reads to buffer on the receiving end. Requires zfs-backup to be installed there.",
        verbose :: w ::: Should BeVerbose <?> "Verbose mode. Prints shell commands."

    }
    | CleanupSnapshots {
        filesystem :: w ::: Remotable (FilesystemName Dst) <?> "Can be \"tank/set\" or \"user@host:tank/set\"",
        mostRecent :: w ::: Maybe Int <?> "Keep most recent N snapshots",
        alsoKeep :: w ::: [History] <?> "To keep 1 snapshot per month for the last 12 months, use \"12@1-per-month\". To keep up to 10 snapshots a day, for the last 10 days, use \"100@10-per-day\", and so on. To keep everything in the last 1.7 years, use \"1.7-years\". Can use day, month, year. Multiple of these flags will result in all the specified snaps being kept. This all works in UTC time, by the way. I'm not dealing with time zones.",
        dryRun :: w ::: Should DryRun <?> "Don't actually do anything, just print what's going to happen",
        ignoring :: w ::: [Regex] <?> "Ignore snapshots with names matching any of these regexes",
        recursive :: w ::: Should OperateRecursively <?> "Recursive mode. Corresponds to `zfs send -R`, `zfs snapshot -r`, `zfs destroy -r`",
        verbose :: w ::: Should BeVerbose <?> "Verbose mode. Prints shell commands."
    }
    | Receive {
        receiveTo :: w ::: SnapshotOrFilesystemName Dst <?> "Can be like \"tank/set\" or \"tank/set@snap\"",
        transferBufferCount :: w ::: Maybe Int <?> "How many reads/writes to buffer between `zfs send` and `zfs receive`. Useful for bursty sends. Default 16",
        verbose :: w ::: Should BeVerbose <?> "Verbose mode. Prints shell commands."

    }
    deriving (Generic)

-- CopySnapshots gets translated to copy-snapshots
instance ParseRecord (Command Wrapped) where
    parseRecord = parseRecordWithModifiers lispCaseModifiers

runCommand :: IO ()
runCommand = do
    command <- unwrapRecord "ZFS Backup Tool"
    case command of
        List host ignoring verbose   -> listPrint verbose host (excluding ignoring) 
        CopySnapshots{..}    -> copy src dst sendCompressed sendRaw dryRun verbose (excluding ignoring) recursive forceFullSend (maybe defaultBufferConfig BufferConfig transferBufferCount) (BufferConfig <$> remoteBufferCount)
        CleanupSnapshots{..} -> cleanup filesystem mostRecent alsoKeep dryRun (excluding ignoring) recursive verbose
        Receive{..}          -> receive verbose receiveTo (maybe defaultBufferConfig BufferConfig transferBufferCount)

excluding :: [Regex] -> SnapshotName sys -> Bool
excluding regexes = \(SnapshotName _fs snap) -> any (identifierOf snap `matches`) regexes
