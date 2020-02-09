module Lib.Progress (printProgress) where

import Control.Concurrent (threadDelay)
import qualified Data.IORef as IORef
import qualified Control.Concurrent.Async as Async
import Text.Printf (printf)

trackProgress :: Int -> (Int -> IO ()) -> ((Int -> IO ()) -> IO a) -> IO a
trackProgress delay report go = do
    counter <- IORef.newIORef 0
    let update i = IORef.atomicModifyIORef' counter (\c -> (c + i, ()))
    done <- Async.async (go update)
    let loop = do
            timer <- Async.async $ threadDelay delay
            Async.waitEither done timer >>= \case
                Left result -> return result
                Right () -> do
                    progress <- IORef.atomicModifyIORef' counter (\c -> (0, c))
                    report progress
                    loop
    loop


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



printProgress :: ((Int -> IO ()) -> IO a) -> IO a
printProgress = trackProgress (1000*1000) (putStrLn . sizeWithUnits "B/sec")
