{-# LANGUAGE MultiParamTypeClasses #-}

module Control.Newtype.C () where

import Control.Newtype
import Data.Int
import Data.Word
import Foreign.C.Types

instance Newtype CChar Int8 where
    pack = CChar
    unpack (CChar x) = x

instance Newtype CSChar Int8 where
    pack = CSChar
    unpack (CSChar x) = x

instance Newtype CUChar Word8 where
    pack = CUChar
    unpack (CUChar x) = x

instance Newtype CShort Int16 where
    pack = CShort
    unpack (CShort x) = x

instance Newtype CUShort Word16 where
    pack = CUShort
    unpack (CUShort x) = x

instance Newtype CInt Int32 where
    pack = CInt
    unpack (CInt x) = x

instance Newtype CUInt Word32 where
    pack = CUInt
    unpack (CUInt x) = x

instance Newtype CLong Int64 where
    pack = CLong
    unpack (CLong x) = x

instance Newtype CULong Word64 where
    pack = CULong
    unpack (CULong x) = x

instance Newtype CPtrdiff Int64 where
    pack = CPtrdiff
    unpack (CPtrdiff x) = x

instance Newtype CSize Word64 where
    pack = CSize
    unpack (CSize x) = x

instance Newtype CWchar Int32 where
    pack = CWchar
    unpack (CWchar x) = x

instance Newtype CSigAtomic Int32 where
    pack = CSigAtomic
    unpack (CSigAtomic x) = x

instance Newtype CLLong Int64 where
    pack = CLLong
    unpack (CLLong x) = x

instance Newtype CULLong Word64 where
    pack = CULLong
    unpack (CULLong x) = x

instance Newtype CIntPtr Int64 where
    pack = CIntPtr
    unpack (CIntPtr x) = x

instance Newtype CUIntPtr Word64 where
    pack = CUIntPtr
    unpack (CUIntPtr x) = x

instance Newtype CIntMax Int64 where
    pack = CIntMax
    unpack (CIntMax x) = x

instance Newtype CUIntMax Word64 where
    pack = CUIntMax
    unpack (CUIntMax x) = x

instance Newtype CClock Int64 where
    pack = CClock
    unpack (CClock x) = x

instance Newtype CTime Int64 where
    pack = CTime
    unpack (CTime x) = x

instance Newtype CUSeconds Word32  where
    pack = CUSeconds
    unpack (CUSeconds x) = x

instance Newtype CSUSeconds Int64 where
    pack = CSUSeconds
    unpack (CSUSeconds x) = x

instance Newtype CFloat Float where
    pack = CFloat
    unpack (CFloat x) = x

instance Newtype CDouble Double where
    pack = CDouble
    unpack (CDouble x) = x
