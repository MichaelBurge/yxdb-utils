module Data.Binary.C () where

import Control.Applicative ((<$>))
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.ReinterpretCast (floatToWord, wordToFloat, doubleToWord, wordToDouble)
import Foreign.C.Types
import GHC.Prim (coerce)

instance Binary CFloat where
    get = coerce <$> wordToFloat <$> getWord32le
    put = putWord32le . floatToWord . coerce

instance Binary CDouble where
    get = coerce <$> wordToDouble <$> getWord64le
    put = putWord64le . doubleToWord . coerce
