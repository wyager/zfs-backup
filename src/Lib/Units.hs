module Lib.Units(TimeUnit(..),Period(..),History(..),binBy,approximateDiffTime) where
import           Lib.Common           (HasParser, parser, unWithParser)

import           Control.Applicative  ((<|>))
import qualified Data.Attoparsec.Text as A
import           Data.Map.Strict      (Map)
import qualified Data.Map.Strict      as Map
import           Data.Ratio           ((%))
import qualified Data.Time.Calendar   as Cal
import           Data.Time.Clock      (UTCTime (UTCTime), NominalDiffTime, nominalDay)
import qualified Data.Time.Clock      as Clock
import           Options.Generic      (ParseField, readField)

data TimeUnit = Day | Month | Year deriving (Eq,Ord)

approximateDiffTime :: TimeUnit -> NominalDiffTime
approximateDiffTime = 
    let d = nominalDay in
    let m = 31 * d in 
    let y = 12 * m in 
    \case 
        Day -> d
        Month -> m
        Year -> y

instance Show TimeUnit where
    show Day   = "day"
    show Month = "month"
    show Year  = "year"

instance HasParser TimeUnit where
    parser = Day <$ "day"
         <|> Month <$ "month"
         <|> Year <$ ("year" <|> "yr")

data Period = Period Integer TimeUnit
instance Show Period where
    show (Period count unit) = show count ++ "-per-" ++ show unit

instance HasParser Period where
    parser = Period <$> A.decimal <*> ("-per-" *> parser)

data History = Sample {countPer :: Int, period :: Period}
             | EverythingFor {howMany :: Rational, unit :: TimeUnit}

instance Show History where
    show (Sample countPer period) = show countPer ++ "@" ++ show period
    show (EverythingFor howMany unit) = show howMany ++ "-" ++ show unit ++ "s"

instance HasParser History where
    parser = sample <|> everything
        where
        sample = Sample <$> A.decimal <*> ("@" *> parser)
        everything = EverythingFor <$> A.rational <*> ("-" *> parser <* "s")

instance ParseField History where
    readField = unWithParser <$> readField


clipTo :: TimeUnit -> UTCTime -> (UTCTime,UTCTime)
clipTo unit time = case unit of
    Day -> let baseline = UTCTime (Clock.utctDay time) 0 in (baseline, Clock.addUTCTime Clock.nominalDay baseline)
    Month -> let (y,m,_d) = Cal.toGregorian (Clock.utctDay time)
                 baseline = Cal.fromGregorian y m 1
                 end = Cal.fromGregorian y m $ Cal.gregorianMonthLength y m
             in (UTCTime baseline 0, Clock.addUTCTime Clock.nominalDay (UTCTime end 0))
    Year -> let (y,_m,_d) = Cal.toGregorian (Clock.utctDay time)
                baseline = Cal.fromGregorian y 1 1
                end = Cal.fromGregorian y 12 31
            in (UTCTime baseline 0, Clock.addUTCTime Clock.nominalDay (UTCTime end 0))

temporalDiv :: TimeUnit -> UTCTime -> (UTCTime, Rational)
temporalDiv unit time = (lo, toRational fraction)
    where
    (lo,hi) = clipTo unit time
    elapsed = time `Clock.diffUTCTime` lo
    maxDur = hi `Clock.diffUTCTime` lo
    fraction = elapsed / maxDur



bin :: Integer -> [(Rational, v)] -> Map Rational (Map Rational v)
bin bins = foldl insert initial
    where
    initial = Map.fromList [(i % bins,Map.empty) | i <- [0..bins-1]]
    insert acc (rat,v) =
        let (lo,at,_) = Map.splitLookup rat acc in
        let key = maybe (error $ "Invalid rational key: " ++ show rat) id $ ((rat <$ at) <|> (fmap (fst . fst) $ Map.maxViewWithKey lo)) in
        Map.adjust (Map.insert rat v) key acc

binBy :: forall v . TimeUnit -> Integer -> [(UTCTime, v)] -> Map UTCTime (Map Rational (Map Rational v))
binBy unit bins times = fmap (bin bins) based
    where
    based :: Map UTCTime [(Rational, v)]
    based = foldl insert Map.empty times
    insert acc (time,v) =
        let (base,frac) = temporalDiv unit time in
        let alter = \case
                Nothing -> Just [(frac,v)]
                Just others -> Just $ (frac,v):others
        in
        Map.alter alter base acc
