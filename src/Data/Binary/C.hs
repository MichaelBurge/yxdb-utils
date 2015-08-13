module Data.Binary.C () where

import Control.Applicative ((<$>))
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.ReinterpretCast (floatToWord, wordToFloat, doubleToWord, wordToDouble)
import Foreign.C.Types

instance Binary CFloat where
    get = CFloat <$> wordToFloat <$> getWord32le
    put = putWord32le . floatToWord . (let f (CFloat x) = x in f)

instance Binary CDouble where
    get = CDouble <$> wordToDouble <$> getWord64le
    put = putWord64le . doubleToWord . (let f (CDouble x) = x in f)
