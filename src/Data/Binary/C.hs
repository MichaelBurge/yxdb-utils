module Data.Binary.C () where

import Control.Applicative ((<$>))
import Control.Newtype(unpack)
import Control.Newtype.C
import Data.Binary
import Data.Binary.Get (getWord8, getWord16le, getWord32le, getWord64le)
import Data.Binary.Put (putWord8, putWord16le, putWord32le, putWord64le)
import Data.ReinterpretCast (floatToWord, wordToFloat, doubleToWord, wordToDouble)
import Foreign.C.Types
import GHC.Prim (coerce)

instance Binary CChar where
    get = fromIntegral <$> getWord8
    put = putWord8 . fromIntegral

instance Binary CSChar where
    get = fromIntegral <$> getWord8
    put = putWord8 . fromIntegral

instance Binary CUChar where
    get = fromIntegral <$> getWord8
    put = putWord8 . fromIntegral


instance Binary CShort where
    get = fromIntegral <$> getWord16le
    put = putWord16le . fromIntegral

instance Binary CUShort where
    get = fromIntegral <$> getWord16le
    put = putWord16le . fromIntegral

instance Binary CInt where
    get = fromIntegral <$> getWord32le
    put = putWord32le . fromIntegral

instance Binary CUInt where
    get = fromIntegral <$> getWord32le
    put = putWord32le . fromIntegral


instance Binary CLong where
    get = fromIntegral <$> getWord64le
    put = putWord64le . fromIntegral

instance Binary CULong where
    get = fromIntegral <$> getWord64le
    put = putWord64le . fromIntegral

instance Binary CPtrdiff where
    get = fromIntegral <$> getWord64le
    put = putWord64le . fromIntegral

instance Binary CSize where
    get = fromIntegral <$> getWord64le
    put = putWord64le . fromIntegral

instance Binary CWchar where
    get = fromIntegral <$> getWord32le
    put = putWord32le . fromIntegral

instance Binary CSigAtomic where
    get = fromIntegral <$> getWord32le
    put = putWord32le . fromIntegral

instance Binary CLLong where
    get = fromIntegral <$> getWord64le
    put = putWord64le . fromIntegral

instance Binary CULLong where
    get = fromIntegral <$> getWord64le
    put = putWord64le . fromIntegral

instance Binary CIntPtr where
    get = fromIntegral <$> getWord64le
    put = putWord64le . fromIntegral

instance Binary CUIntPtr where
    get = fromIntegral <$> getWord64le
    put = putWord64le . fromIntegral

instance Binary CIntMax where
    get = fromIntegral <$> getWord64le
    put = putWord64le . fromIntegral

instance Binary CUIntMax where
    get = fromIntegral <$> getWord64le
    put = putWord64le . fromIntegral

instance Binary CClock where
    get = fromIntegral <$> getWord64le
    put = putWord64le . fromIntegral . unpack

instance Binary CTime where
    get = fromIntegral <$> getWord64le
    put = putWord64le . fromIntegral . unpack

instance Binary CUSeconds where
    get = fromIntegral <$> getWord32le
    put = putWord32le . fromIntegral . unpack

instance Binary CSUSeconds where
    get = fromIntegral <$> getWord64le
    put = putWord64le . fromIntegral . unpack

instance Binary CFloat where
    get = coerce <$> wordToFloat <$> getWord32le
    put = putWord32le . floatToWord . coerce

instance Binary CDouble where
    get = coerce <$> wordToDouble <$> getWord64le
    put = putWord64le . doubleToWord . coerce
