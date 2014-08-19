module Tests.Codec.Compression.LZF.ByteString (lzfByteStringTests) where

import Codec.Compression.LZF.ByteString
    (
     compressByteString,
     compressLazyByteString,
     decompressByteString,
     decompressByteStringFixed,
     decompressLazyByteString
    )

import Control.Applicative((<$>))

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL

import Test.Framework
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck
import Test.QuickCheck.Instances
import Test.QuickCheck.Monadic (assert, monadic, run)

_compare :: Eq t => (t -> t) -> (t -> t) -> t -> Property
_compare a b x = property $ x == (a $ b x)

prop_DecompressCompressInverses :: BS.ByteString -> Property
prop_DecompressCompressInverses x =
    _compare decompressByteString compressByteString x

prop_LazyDecompressCompressInverses :: BSL.ByteString -> Property
prop_LazyDecompressCompressInverses x =
    _compare decompressLazyByteString compressLazyByteString x

data NSmall = NSmall Int deriving (Show)
data BSSmall = BSSmall BS.ByteString deriving (Show)

instance Arbitrary NSmall where
    arbitrary = NSmall <$> choose (0,10000)

instance Arbitrary BSSmall where
    arbitrary = do
      size <- choose(0,10000)
      BSSmall <$> BS.pack <$> vector size

prop_DecompressingAnyGarbageDoesntCauseACrash :: NSmall -> BSSmall -> Property
prop_DecompressingAnyGarbageDoesntCauseACrash (NSmall n) (BSSmall bs) =
    property $
    case decompressByteStringFixed n bs of
      Nothing -> True
      Just x -> BS.length x >= 0

lzfByteStringTests =
    testGroup "Codec.Compression.LZF.ByteString" [
        testProperty "Decompress and compress inverses" prop_DecompressCompressInverses,
        testProperty "Lazy Decompress and compress inverses" prop_LazyDecompressCompressInverses,
        testProperty "Checking for crashes on arbitrary ByteStrings" prop_DecompressingAnyGarbageDoesntCauseACrash
    ]
