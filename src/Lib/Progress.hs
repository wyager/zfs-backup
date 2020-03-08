module Lib.Progress (printProgress) where

import           Control.Concurrent       (threadDelay)
import qualified Control.Concurrent.Async as Async
import qualified Data.IORef               as IORef
import           Text.Printf              (printf)

trackProgress :: Int -> s -> (Int -> Int -> s -> IO ()) -> ((Int -> s -> IO ()) -> IO a) -> IO a
trackProgress delay state report go = do
    counter <- IORef.newIORef (0,state)
    let update i s = IORef.atomicModifyIORef' counter (\(c,_s) -> ((c + i,s), ()))
    done <- Async.async (go update)
    let loop total = do
            timer <- Async.async $ threadDelay delay
            Async.waitEither done timer >>= \case
                Left result -> return result
                Right () -> do
                    (progress,s) <- IORef.atomicModifyIORef' counter (\(c,s) -> ((0,s), (c,s)))
                    report (total + progress) progress s
                    loop (total + progress)
    loop 0


sizeWithUnits :: Integral i => String -> i -> String
sizeWithUnits unit i = printf "%.1f %s%s" scaled prefix unit
    where
    f = fromIntegral i :: Double
    thousands = floor (log f / log 1000) :: Word
    (prefix :: String, divisor :: Double) = case thousands of
        0 -> ("",  1e0)
        1 -> ("k", 1e3)
        2 -> ("M", 1e6)
        3 -> ("G", 1e9)
        _ -> ("T", 1e12)
    scaled = f / divisor

detailed :: (Integral i, Show s) => String -> String -> String -> i -> i -> s -> String
detailed label unit period total progress s = printf "%s: %s, %s (%s)" label total' progress' (show s)
    where
    progress' = sizeWithUnits (unit ++ "/" ++ period) progress
    total' = sizeWithUnits unit total


printProgress :: Show s => String -> s -> ((Int -> s -> IO ()) -> IO a) -> IO a
printProgress label s0 = trackProgress
    (1000*1000) 
    s0
    (\total prog s -> putStrLn $ detailed label "B" "sec" total prog s)
