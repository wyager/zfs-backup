module Lib.ZFS (
    Size, GUID, ObjectMeta(..), Object(..), 
    FilesystemName(..), SnapshotIdentifier(..), SnapshotName(..), 
    objects, listShellCmd, listSnapsShellCmd, rehome,
    ObjSet, snapshots, withFS, presentIn, byDate, single, seconds, byGUID) where
import           Lib.Common            (HasParser, parser, unWithParser)

import           Control.Applicative   (many, (<|>))
import qualified Data.Attoparsec.Text  as A
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
import           Data.Typeable         (Typeable)

newtype Size = Size Word64 deriving newtype (Eq, Ord, Show, Num)
instance HasParser Size where
    parser = Size <$> A.decimal

newtype GUID = GUID Word64 deriving newtype (Eq, Ord, Show)
instance HasParser GUID where
    parser = GUID <$> A.decimal

data ObjectMeta = ObjectMeta
    { creationOf   :: !UTCTime
    , guidOf       :: !GUID
    , referencedOf :: !Size
    , usedOf       :: !Size
    } deriving (Eq, Ord, Show)

data Object sys = Filesystem (FilesystemName sys) ObjectMeta
                | Volume
                | Snapshot (SnapshotName sys) ObjectMeta
            deriving (Eq, Ord, Show)



newtype FilesystemName sys = FilesystemName Text
    deriving stock (Eq, Ord, Generic)
    deriving anyclass ParseRecord


instance HasParser (FilesystemName sys) where
    parser = FilesystemName <$> A.takeWhile (not . A.inClass " @\t")


instance Show (FilesystemName sys) where
    show (FilesystemName name) = T.unpack name

instance Typeable sys => ParseField (FilesystemName sys) where
    readField = unWithParser <$> readField
deriving anyclass instance Typeable sys => ParseFields (FilesystemName sys)

newtype SnapshotIdentifier sys = SnapshotIdentifier {identifierOf :: Text} deriving (Eq,Ord)
instance Show (SnapshotIdentifier sys) where
    show (SnapshotIdentifier ident) = T.unpack ident

rehome :: SnapshotIdentifier a -> SnapshotIdentifier b
rehome = SnapshotIdentifier . identifierOf


data SnapshotName sys = SnapshotName {snapshotFSOf :: FilesystemName sys, snapshotNameOf :: SnapshotIdentifier sys} deriving (Eq,Ord)
instance Show (SnapshotName sys) where
    show (SnapshotName fs snap) = show fs ++ "@" ++ show snap

instance HasParser (SnapshotName sys) where
    parser = do
        snapshotFSOf <- parser
        snapshotNameOf <- SnapshotIdentifier <$> ("@" *> A.takeWhile (not . A.inClass " \t"))
        return SnapshotName{..}

instance Typeable sys => ParseField (SnapshotName sys) where
    readField = unWithParser <$> readField

instance HasParser (Object sys) where
    parser = (fs <|> vol <|> snap) <* A.endOfLine
        where
        fs = "filesystem" *> (Filesystem <$> t parser <*> t meta)
        vol = "volume" *> (Volume <$ A.takeTill A.isEndOfLine)
        snap = "snapshot" *> (Snapshot <$> t parser <*> t meta)
        meta = ObjectMeta <$> creation <*> t parser <*> t parser <*> t parser
        t x = A.char '\t' *> x
        creation = seconds <$> A.decimal

objects :: A.Parser [Object sys]
objects = many parser <* A.endOfInput

seconds :: Word64 -> UTCTime
seconds = posixSecondsToUTCTime . fromIntegral

listShellCmd :: String
listShellCmd = "zfs list -Hp -t all -o type,name,creation,guid,referenced,used"

listSnapsShellCmd :: FilesystemName sys -> String
listSnapsShellCmd  fs = "zfs list -Hp -t snapshot -o type,name,creation,guid,referenced,used " ++ show fs


newtype ObjSet name = ObjSet {getSnapSet :: Map GUID (Map name ObjectMeta)} deriving (Show)

byGUID :: GUID -> ObjSet name -> Maybe (Map name ObjectMeta)
byGUID guid (ObjSet objs) = Map.lookup guid objs 

snapshots :: [Object sys] -> ObjSet (SnapshotName sys)
snapshots objs = ObjSet $ Map.fromListWith Map.union [(guidOf meta, Map.singleton name meta) | Snapshot name meta <- objs]

withFS :: forall sys . FilesystemName sys -> ObjSet (SnapshotName sys) -> ObjSet (SnapshotIdentifier sys)
withFS fsName (ObjSet objs) =
    ObjSet $ Map.filter (not . null) $ Map.map pullOutRelevant objs
    where
    pullOutRelevant :: Map (SnapshotName sys) v -> Map (SnapshotIdentifier sys) v
    pullOutRelevant snaps = Map.fromList [(ident,v) | (SnapshotName fs ident, v) <- Map.toList snaps, fs == fsName]


presentIn :: ObjSet (k sys) -> ObjSet (k sys2) -> ObjSet (k sys)
(ObjSet a) `presentIn` (ObjSet b) = ObjSet (Map.intersection a b)

triplets :: ObjSet (name sys) -> [(GUID, name sys, UTCTime)]
triplets (ObjSet snaps) = [(guid,name,creationOf meta) | (guid,snaps') <- Map.toList snaps, (name,meta) <- Map.toList snaps']

byDate :: Ord (name sys) => ObjSet (name sys) -> Map UTCTime (Set (GUID, name sys))
byDate = Map.fromListWith Set.union . map (\(guid,name,time) -> (time,Set.singleton (guid,name))) . triplets


single :: Show v => Set (GUID, v) -> Either String (GUID, v)
single snaps = case Set.minView snaps of
    Nothing -> Left "Error: Zero available matching snaps"
    Just (lowest,others) | null others -> Right lowest
                      | otherwise -> Left $ "Error: Ambiguity between snaps: " ++ show lowest ++ " " ++ show others

