module Main where

import Lib
import System.Exit (exitFailure)
import Control.Exception (onException)

main :: IO ()
main = runCommand `onException` exitFailure
