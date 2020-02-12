module Lib.Regex (Regex, matches) where

import qualified Text.Regex.TDFA as RT
import           Text.Regex.TDFA.Text ()
import qualified Text.Regex.Base as RB
import qualified Options.Applicative  as Opt
import           Options.Generic      (ParseField, readField)
import           Data.Text (Text)

newtype Regex = Regex RT.Regex

instance ParseField Regex where
    readField = Regex <$> Opt.eitherReader RB.makeRegexM

matches :: Text -> Regex -> Bool
text `matches` (Regex regex) = RB.matchTest regex text