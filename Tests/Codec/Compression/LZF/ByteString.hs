module Tests.Codec.Compression.LZF.ByteString (lzfByteStringTests) where

import Codec.Compression.LZF.ByteString
    (
     compressByteString,
     compressLazyByteString,
     decompressByteString,
     decompressLazyByteString
    )

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

lzfByteStringTests =
    testGroup "Codec.Compression.LZF.ByteString" [
        testProperty "Decompress and compress inverses" prop_DecompressCompressInverses,
        testProperty "Lazy Decompress and compress inverses" prop_LazyDecompressCompressInverses
    ]
