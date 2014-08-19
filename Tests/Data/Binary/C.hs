module Tests.Data.Binary.C (cBinaryTests) where

import Control.Newtype as NT
import Control.Newtype.C
import Data.Binary
import Data.Binary.C
import Foreign.C.Types

import Test.Framework
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck
import Test.QuickCheck.Instances
import Test.QuickCheck.Monadic (assert, monadic, run)

checkConsistency :: (Eq n, Binary n, Binary o, Newtype n o) => n -> o -> Property
checkConsistency x y =
    let p = pack y
    in property $ ((decode $ encode p) `asTypeOf` x) == p

cBinaryTests =
    testGroup "Binary instances for C types" [
        testProperty "CChar"      $ checkConsistency (undefined::CChar),
        testProperty "CSChar"     $ checkConsistency (undefined::CSChar),
        testProperty "CUChar"     $ checkConsistency (undefined::CUChar),
        testProperty "CShort"     $ checkConsistency (undefined::CShort),
        testProperty "CUShort"    $ checkConsistency (undefined::CUShort),
        testProperty "CInt"       $ checkConsistency (undefined::CInt),
        testProperty "CUInt"      $ checkConsistency (undefined::CUInt),
        testProperty "CLong"      $ checkConsistency (undefined::CLong),
        testProperty "CULong"     $ checkConsistency (undefined::CULong),
        testProperty "CPtrdiff"   $ checkConsistency (undefined::CPtrdiff),
        testProperty "CSize"      $ checkConsistency (undefined::CSize),
        testProperty "CWchar"     $ checkConsistency (undefined::CWchar),
        testProperty "CSigAtomic" $ checkConsistency (undefined::CSigAtomic),
        testProperty "CLLong "    $ checkConsistency (undefined::CLLong),
        testProperty "CULLong"    $ checkConsistency (undefined::CULLong),
        testProperty "CIntPtr"    $ checkConsistency (undefined::CIntPtr),
        testProperty "CUIntPt"    $ checkConsistency (undefined::CUIntPtr),
        testProperty "CIntMax"    $ checkConsistency (undefined::CIntMax),
        testProperty "CUIntMax"   $ checkConsistency (undefined::CUIntMax),
        testProperty "CClock"     $ checkConsistency (undefined::CClock),
        testProperty "CTime"      $ checkConsistency (undefined::CTime),
        testProperty "CUSeconds"  $ checkConsistency (undefined::CUSeconds),
        testProperty "CSUSeconds" $ checkConsistency (undefined::CSUSeconds),
        testProperty "CFloat"     $ checkConsistency (undefined::CFloat),
        testProperty "CDouble"    $ checkConsistency (undefined::CDouble)
    ]
