module Lib.Command.Delete (cleanup) where
import qualified Control.Exception    as Ex
import           Control.Monad        (unless, when)
import           Data.Bifunctor       (second)
import           Data.Map.Strict      (Map)
import qualified Data.Map.Strict      as Map
import           Data.Set             (Set)
import qualified Data.Set             as Set
import           Data.Time.Clock      (UTCTime, getCurrentTime, addUTCTime)
import           Lib.Common           (Remotable, SSHSpec, remotable, thing,
                                       Should, should, DryRun, OperateRecursively, BeVerbose)
import           Lib.Command.List     (list)
import           Lib.Units            (History (..), Period (..), binBy, approximateDiffTime)
import           Lib.ZFS              (FilesystemName, ObjSet,
                                       SnapshotName (..), byDate, single,
                                       snapshots, withFS, SnapshotIdentifier(..))
import qualified System.Process.Typed as P
import           Text.Printf          (printf)
import           Data.List            (intercalate)

data DeleteError = Couldn'tPlan String | NoSuchFilesystem deriving (Show, Ex.Exception)

cleanup :: Remotable (FilesystemName sys) -> Maybe Int -> [History] -> Should DryRun -> (SnapshotName sys -> Bool) -> Should OperateRecursively -> Should BeVerbose -> IO ()
cleanup filesystem mostRecent alsoKeep dryRun excluding recursively verbose = do
    let remote = remotable Nothing Just filesystem
    snaps <- either Ex.throw (return . maybe (Ex.throw NoSuchFilesystem) snapshots) =<< list verbose (Just $ thing filesystem) remote excluding
    now <- getCurrentTime
    plan <- either (Ex.throw . Couldn'tPlan) return $ planDeletion now (thing filesystem) (withFS (thing filesystem) snaps) (maybe 0 id mostRecent) alsoKeep
    putStrLn $ prettyDeletePlan plan recursively
    unless (should @DryRun dryRun) $ executeDeletePlan remote plan recursively verbose

executeDeletePlan :: Maybe SSHSpec -> DeletePlan sys -> Should OperateRecursively -> Should BeVerbose -> IO ()
executeDeletePlan delSpec delPlan recursively verbose = 
    case deleteCommand delSpec delPlan recursively of
        Nothing -> return ()
        Just (delExe, delArgs) -> do
            let delProc = P.setStdin P.closed $ P.proc delExe delArgs
            when (should @BeVerbose verbose) $ print delProc
            P.withProcessWait_ delProc $ \_del -> return ()

data DeletePlan sys = DeletePlan {deleteFS :: FilesystemName sys, toDelete :: Set (SnapshotIdentifier sys), toKeep :: Set (SnapshotIdentifier sys)} deriving Show

prettyDeletePlan :: DeletePlan sys -> Should OperateRecursively -> String
prettyDeletePlan (DeletePlan fs delete keep) recursively = concat
    ([ printf "Deleting %i snaps\n" (length delete)
    , printf "Keeping %i snaps:\n" (length keep)
    , concatMap (printf "  %s\n" . show . SnapshotName fs) (Set.toList keep)
    ] ++ if should recursively then ["Operating in recursive mode (destroy -r)\n"] else [])


planDeletion :: forall sys . UTCTime -> FilesystemName sys -> ObjSet (SnapshotIdentifier sys) -> Int -> [History] -> Either String (DeletePlan sys)
planDeletion now fsName snapSet mostRecentN histories = do
    inOrder :: Map UTCTime (SnapshotIdentifier sys) <- mapM (second snd . single) $ byDate snapSet
    let mostRecent = Set.fromList $ map snd $ take mostRecentN $ Map.toDescList inOrder
    let keepHistories = Set.unions $ map (keepHistory now inOrder) histories
    let toKeep = Set.union keepHistories mostRecent
    let toDelete = (Set.fromList $ Map.elems inOrder) `Set.difference` toKeep
    return (DeletePlan fsName toDelete toKeep)


keepHistory :: forall v . (Ord v) => UTCTime ->  Map UTCTime v -> History -> Set v
keepHistory now snaps (EverythingFor howMany unit) = Set.fromList (Map.elems newer)
    where
    difference = approximateDiffTime unit * fromRational howMany
    startTime = negate difference `addUTCTime` now
    (_older, newer) = Map.split startTime snaps
keepHistory _   snaps (Sample count (Period frac timeUnit)) = Set.fromList $ take count best
    where
    -- Outer map - "Base time" (e.g. day boundaries if time unit is day)
    -- Middle map - Start of each bin
    -- Inner map - actual time
    binned :: Map UTCTime (Map Rational (Map Rational v))
    binned = binBy timeUnit frac $ Map.toList snaps
    best :: [v]
    best = do
        (_base, perBase) <- Map.toDescList binned
        (_frac, perFrac) <- Map.toDescList perBase
        case Map.maxView perFrac of
            Nothing    -> []
            Just (v,_) -> [v]

format :: FilesystemName sys -> Set (SnapshotIdentifier sys) -> String
format fs snaps = show fs ++ "@" ++ intercalate "," (map show $ Set.toList snaps)

deleteCommand :: Maybe SSHSpec -> DeletePlan sys -> Should OperateRecursively -> Maybe (String, [String])
deleteCommand ssh (DeletePlan fs del _keep) recursively
    | null del = Nothing
    | otherwise = Just $ case ssh of
        Nothing   -> ("zfs", ["destroy"] ++ recFlag ++  [format fs del])
        Just spec -> ("ssh", [show spec, "zfs"] ++ ["destroy"] ++ recFlag ++ [format fs del])
    where
    recFlag = if should @OperateRecursively recursively then ["-r"] else []
