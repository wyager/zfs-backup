module Lib.Command.Delete (cleanup) where
import qualified Control.Exception    as Ex
import           Control.Monad        (unless)
import           Data.Bifunctor       (second)
import           Data.Map.Strict      (Map)
import qualified Data.Map.Strict      as Map
import           Data.Set             (Set)
import qualified Data.Set             as Set
import           Data.Time.Clock      (UTCTime, getCurrentTime, addUTCTime)
import           Lib.Common           (Remotable, SSHSpec, remotable, thing)
import           Lib.Command.List     (list)
import           Lib.Units            (History (..), Period (..), binBy, approximateDiffTime)
import           Lib.ZFS              (FilesystemName, SnapSet,
                                       SnapshotName (..), byDate, single,
                                       snapshots, withFS)
import qualified System.Process.Typed as P
import           Text.Printf          (printf)

data DeleteError = Couldn'tPlan String deriving (Show, Ex.Exception)

cleanup :: Remotable FilesystemName -> Maybe Int -> [History] -> Bool -> (SnapshotName -> Bool) -> IO ()
cleanup filesystem mostRecent alsoKeep dryRun excluding = do
    let remote = remotable Nothing Just filesystem
    snaps <- either Ex.throw (return . snapshots) =<< list remote excluding
    now <- getCurrentTime
    plan <- either (Ex.throw . Couldn'tPlan) return $ planDeletion now (thing filesystem) snaps (maybe 0 id mostRecent) alsoKeep
    putStrLn $ prettyDeletePlan plan
    unless dryRun $ executeDeletePlan remote plan

executeDeletePlan :: Maybe SSHSpec -> DeletePlan -> IO ()
executeDeletePlan delSpec DeletePlan{..} = flip mapM_ toDelete $ \snapshot -> do
    let (delExe, delArgs) = deleteCommand delSpec snapshot
    let delProc = P.setStdin P.closed $ P.proc delExe delArgs
    print delProc
    P.withProcessWait_ delProc $ \_del -> return ()


data DeletePlan = DeletePlan {toDelete :: Set SnapshotName, toKeep :: Set SnapshotName} deriving Show

prettyDeletePlan :: DeletePlan -> String
prettyDeletePlan (DeletePlan delete keep) = concat
    [ printf "Deleting %i snaps\n" (length delete)
    , printf "Keeping %i snaps:\n" (length keep)
    , concatMap (printf "  %s\n" . show) (Set.toList keep)
    ]


planDeletion :: UTCTime -> FilesystemName -> SnapSet -> Int -> [History] -> Either String DeletePlan
planDeletion now fsName snapSet mostRecentN histories = do
    inOrder :: Map UTCTime SnapshotName <- mapM (second snd . single) $ byDate $ withFS fsName snapSet
    let mostRecent = Set.fromList $ map snd $ take mostRecentN $ Map.toDescList inOrder
    let keepHistories = Set.unions $ map (keepHistory now inOrder) histories
    let toKeep = Set.union keepHistories mostRecent
    let toDelete = (Set.fromList $ Map.elems inOrder) `Set.difference` toKeep
    return (DeletePlan toDelete toKeep)


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

deleteCommand :: Maybe SSHSpec -> SnapshotName -> (String, [String])
deleteCommand ssh snap = case ssh of
    Nothing   -> ("zfs", ["destroy", show snap])
    Just spec -> ("ssh", [show spec, "zfs", "destroy", show snap])
