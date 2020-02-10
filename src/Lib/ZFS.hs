module Lib.ZFS (Size, PoolName, GUID, ObjectMeta(..), Object(..), FilesystemName(..), SnapshotName(..), objects, listShellCmd, SnapSet, snapshots, withFS, presentIn, byDate, single) where
import           Lib.Common            (HasParser, parser, unWithParser)

import           Control.Applicative   (many, (<|>))
import qualified Data.Attoparsec.Text  as A
import           Data.ByteString       (ByteString)
import           Data.Map.Strict       (Map)
import qualified Data.Map.Strict       as Map
import           Data.Set              (Set)
import qualified Data.Set              as Set
import           Data.Text             (Text)
import qualified Data.Text             as T
import           Data.Time.Clock       (UTCTime)
import           Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import           Data.Word             (Word64)
import           GHC.Generics          (Generic)
import           Options.Generic       (ParseField, ParseFields, ParseRecord,
                                        readField)

newtype Size = Size Word64 deriving newtype (Eq, Ord, Show, Num)

newtype PoolName = PoolName ByteString deriving newtype (Eq, Ord, Show)

newtype GUID = GUID Word64 deriving newtype (Eq, Ord, Show)

data ObjectMeta = ObjectMeta
    { creationOf   :: !UTCTime
    , guidOf       :: !GUID
    , referencedOf :: !Size
    , usedOf       :: !Size
    } deriving (Eq, Ord, Show)

data Object = Filesystem FilesystemName ObjectMeta
            | Volume
            | Snapshot SnapshotName ObjectMeta
            deriving (Eq, Ord, Show)



newtype FilesystemName = FilesystemName Text
    deriving stock (Eq, Ord, Generic)
    deriving anyclass ParseRecord


instance HasParser FilesystemName where
    parser = FilesystemName <$> A.takeWhile (not . A.inClass " @\t")


instance Show FilesystemName where
    show (FilesystemName name) = T.unpack name

instance ParseField FilesystemName where
    readField = unWithParser <$> readField
deriving anyclass instance ParseFields FilesystemName



data SnapshotName = SnapshotName {snapshotFSOf :: FilesystemName, snapshotNameOf :: Text} deriving (Eq,Ord)
instance Show SnapshotName where
    show (SnapshotName fs snap) = show fs ++ "@" ++ T.unpack snap

instance HasParser SnapshotName where
    parser = do
        snapshotFSOf <- parser
        snapshotNameOf <- "@" *> A.takeWhile (not . A.inClass " \t")
        return SnapshotName{..}

instance ParseField SnapshotName where
    readField = unWithParser <$> readField

instance HasParser Object where
    parser = (fs <|> vol <|> snap) <* A.endOfLine
        where
        fs = "filesystem" *> (Filesystem <$> t parser <*> t meta)
        vol = "volume" *> (Volume <$ A.takeTill A.isEndOfLine)
        snap = "snapshot" *> (Snapshot <$> t parser <*> t meta)
        meta = ObjectMeta <$> creation <*> t guid <*> t size <*> t size
        t x = A.char '\t' *> x
        creation = seconds <$> A.decimal
        guid = GUID <$> A.decimal
        size = Size <$> A.decimal

objects :: A.Parser [Object]
objects = many parser <* A.endOfInput

seconds :: Word64 -> UTCTime
seconds = posixSecondsToUTCTime . fromIntegral

listShellCmd :: String
listShellCmd = "zfs list -Hp -t all -o type,name,creation,guid,referenced,used"


newtype SnapSet = SnapSet {getSnapSet :: Map GUID (Map SnapshotName ObjectMeta)} deriving (Show)

snapshots :: [Object] -> SnapSet
snapshots objs = SnapSet $ Map.fromListWith Map.union [(guidOf meta, Map.singleton name meta) | Snapshot name meta <- objs]




withFS :: FilesystemName -> SnapSet -> SnapSet
withFS fsName (SnapSet snaps) =
    SnapSet $ Map.filter (not . null) $ Map.map (Map.filterWithKey relevant) snaps
    where
    relevant name _meta = snapshotFSOf name == fsName


presentIn :: SnapSet -> SnapSet -> SnapSet
(SnapSet a) `presentIn` (SnapSet b) = SnapSet (Map.intersection a b)


triplets :: SnapSet -> [(GUID, SnapshotName, UTCTime)]
triplets (SnapSet snaps) = [(guid,name,creationOf meta) | (guid,snaps') <- Map.toList snaps, (name,meta) <- Map.toList snaps']

byDate :: SnapSet -> Map UTCTime (Set (GUID, SnapshotName))
byDate = Map.fromListWith Set.union . map (\(guid,name,time) -> (time,Set.singleton (guid,name))) . triplets


single :: Set (GUID, SnapshotName) -> Either String (GUID, SnapshotName)
single snaps = case Set.minView snaps of
    Nothing -> Left "Error: Zero available matching snaps. Bug?"
    Just (lowest,others) | null others -> Right lowest
                      | otherwise -> Left $ "Error: Ambiguity between snaps: " ++ show lowest ++ " " ++ show others

