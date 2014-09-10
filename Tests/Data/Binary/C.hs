module Tests.Data.Binary.C (cBinaryTests) where

import Control.Applicative
import Data.Binary
import Data.Binary.C
import Foreign.C.Types
import GHC.Prim (coerce)
import Test.Framework
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck
import Test.QuickCheck.Instances
import Test.QuickCheck.Monadic (assert, monadic, run)

prop_CFloat :: CFloat -> Property
prop_CFloat x = property $ (decode $ encode x) == x

prop_CDouble :: CDouble -> Property
prop_CDouble x = property $ (decode $ encode x) == x

instance Arbitrary CFloat where
  arbitrary = realToFrac <$> (arbitrary :: Gen Float)

instance Arbitrary CDouble where
  arbitrary = realToFrac <$> (arbitrary :: Gen Double)

cBinaryTests =
    testGroup "Binary instances for C types" [
        testProperty "CFloat" prop_CFloat,
        testProperty "CDouble" prop_CDouble
    ]
