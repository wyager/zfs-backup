module Lib.Progress (printProgress) where

import           Control.Concurrent       (threadDelay)
import qualified Control.Concurrent.Async as Async
import qualified Data.IORef               as IORef
import           Text.Printf              (printf)

trackProgress :: Int -> (Int -> Int -> IO ()) -> ((Int -> IO ()) -> IO a) -> IO a
trackProgress delay report go = do
    counter <- IORef.newIORef 0
    let update i = IORef.atomicModifyIORef' counter (\c -> (c + i, ()))
    done <- Async.async (go update)
    let loop total = do
            timer <- Async.async $ threadDelay delay
            Async.waitEither done timer >>= \case
                Left result -> return result
                Right () -> do
                    progress <- IORef.atomicModifyIORef' counter (\c -> (0, c))
                    report (total + progress) progress
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

detailed :: Integral i => String -> String -> String -> i -> i -> String
detailed label unit period total progress = printf "%s: %s, %s" label total' progress'
    where
    progress' = sizeWithUnits (unit ++ "/" ++ period) progress
    total' = sizeWithUnits unit total


printProgress :: String -> ((Int -> IO ()) -> IO a) -> IO a
printProgress label = trackProgress 
    (1000*1000) 
    (\total prog -> putStrLn $ detailed label "B" "sec" total prog)
