
module Lib.Common(
    HasParser, parser, unWithParser, 
    Host(..), SSHSpec(..), Remotable(..), 
    thing, remotable, Src, Dst, Should, 
    should, SendCompressed, SendRaw, 
    DryRun, OperateRecursively, ForceFullSend,
    BeVerbose, UseFreeBSDMode, yes) where

import           Control.Applicative  ((<|>))
import qualified Data.Attoparsec.Text as A
import           Data.Bifunctor       (first)
import           Data.Text            (Text)
import qualified Data.Text            as T
import           Data.Typeable        (Typeable)
import           GHC.Generics         (Generic)
import qualified Net.IPv4             as IP4
import qualified Net.IPv6             as IP6
import qualified Options.Applicative  as Opt
import           Options.Generic      (Only (fromOnly), ParseField, ParseFields,
                                       ParseRecord, parseRecord, readField, parseFields)

data Dst
data Src

yes :: forall a . Should a
yes = Should True

newtype Should a = Should {should :: Bool}
instance ParseFields (Should a) where
    parseFields m l n = Should <$> parseFields m l n
instance ParseRecord (Should a) where
    parseRecord = Should <$> parseRecord

data SendCompressed
data SendRaw
data DryRun
data OperateRecursively
data ForceFullSend
data BeVerbose
data UseFreeBSDMode


class HasParser a where
    parser :: A.Parser a

newtype WithParser a = WithParser {unWithParser :: a}

instance (Typeable a, HasParser a) => ParseField (WithParser a) where
    readField = Opt.eitherReader (first ("Parse error: " ++ ) . A.parseOnly ((WithParser <$> parser) <* A.endOfInput) . T.pack)

data Host = IPv6Host IP6.IPv6 | IPv4Host IP4.IPv4 | TextHost Text

instance HasParser Host where
    parser = (IPv6Host <$> IP6.parser)
         <|> (IPv4Host <$> IP4.parser)
         <|> (TextHost <$> A.takeWhile (not . A.inClass " @:\t"))

instance Show Host where
    show (IPv6Host ip) = T.unpack $ IP6.encode ip
    show (IPv4Host ip) = T.unpack $ IP4.encode ip
    show (TextHost h)  = T.unpack h

data SSHSpec = SSHSpec {
    user :: Maybe Text,
    host :: Host
} deriving (Generic)


instance Show SSHSpec where
    show SSHSpec{..} = case user of
        Nothing  -> show host
        Just usr -> T.unpack usr ++ "@" ++ show host

instance ParseField SSHSpec where
    readField = unWithParser <$> readField

instance HasParser SSHSpec where
    parser = do
        let reserved = A.inClass " @/"
        user <- (Just <$> A.takeWhile (not . reserved) <* "@") <|> pure Nothing
        host <- parser
        return SSHSpec{..}

data Remotable a
    = Remote SSHSpec a
    | Local a
    deriving Generic

instance (HasParser a, Typeable a) => ParseRecord (Remotable a) where
    parseRecord = fromOnly <$> parseRecord

thing :: Remotable a -> a
thing (Remote _ a) = a
thing (Local a)    = a

remotable :: a -> (SSHSpec -> a) -> Remotable x -> a
remotable _ f (Remote spec _) = f spec
remotable def _ (Local _)     = def

instance Show a => Show (Remotable a) where
    show (Local a)       = show a
    show (Remote spec a) = show spec ++ ":" ++ show a

instance HasParser a => HasParser (Remotable a) where
    parser = remote <|> local
        where
        remote = Remote <$> (parser <* ":") <*> parser
        local = Local <$> parser

instance (HasParser a, Typeable a) => ParseField (Remotable a) where
    readField = unWithParser <$> readField
instance (HasParser a, Typeable a) => ParseFields (Remotable a)


