module Tests.Database.Alteryx (yxdbTests) where

import Database.Alteryx
import Tests.Database.Alteryx.Arbitrary

import Prelude hiding (readFile)

import Control.Applicative
import Control.Lens hiding (elements)
import Control.Monad (replicateM, when)
import Data.Array.IArray (listArray)
import Data.Binary
import Data.Binary.Get (runGet)
import Data.Binary.Put (runPut)
import Data.ByteString as BS
import Data.ByteString.Lazy as BSL
import Test.Framework
import Test.Framework.Providers.HUnit (testCase)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.HUnit
import Test.QuickCheck

exampleFilename :: String
exampleFilename = "small-module.yxdb"

assertEq :: (Eq a, Show a) => a -> a -> Property
assertEq a b = let
    aStr = show a
    bStr = show b
    in printTestCase ("\nA: " ++ aStr ++ "\nB: " ++ bStr) (a == b)

numBytes :: Binary a => a -> Int
numBytes x = fromIntegral $ BSL.length $ runPut $ put x

prop_HeaderLength :: Header -> Property
prop_HeaderLength header =
    assertEq headerPageSize (numBytes header)

prop_MetadataLength :: YxdbFile -> Property
prop_MetadataLength yxdb =
    assertEq (numMetadataBytesHeader $ yxdb ^. yxdbFileHeader)
             (numMetadataBytesActual $ yxdb ^. yxdbFileMetadata)

-- prop_BlocksLength :: YxdbFile -> Property
-- prop_BlocksLength yxdb =
--     assertEq (numBlocksBytesHeader $ header yxdb) (numBlocksBytesActual $ blocks yxdb)

prop_ValueGetAndPutAreInverses :: PairedValue -> Property
prop_ValueGetAndPutAreInverses (PairedValue field value) =
    assertEq (runGet (getValue field) (runPut $ putValue field value)) value

prop_BlockIndexGetAndPutAreInverses :: BlockIndex -> Property
prop_BlockIndexGetAndPutAreInverses x = assertEq (decode $ encode x) x

prop_MetadataGetAndPutAreInverses :: RecordInfo -> Property
prop_MetadataGetAndPutAreInverses x = assertEq (decode $ encode x) x

prop_HeaderGetAndPutAreInverses :: Header -> Property
prop_HeaderGetAndPutAreInverses x = assertEq (decode $ encode x) x

prop_BlocksGetAndPutAreInverses :: Block -> Property
prop_BlocksGetAndPutAreInverses x = assertEq (decode $ encode x) x

prop_YxdbFileGetAndPutAreInverses :: YxdbFile -> Property
prop_YxdbFileGetAndPutAreInverses x = assertEq (decode $ encode x) x

test_LoadingSmallModule :: Assertion
test_LoadingSmallModule = do
  parsed <- decodeFileOrFail exampleFilename
  case parsed of
    Left (bytes, msg) -> assertFailure (msg ++ " at " ++ show bytes ++ " bytes")
    Right yxdbFile -> const (return ()) (yxdbFile :: YxdbFile)

yxdbTests :: Test.Framework.Test
yxdbTests =
    testGroup "YXDB" [
        testProperty "Header length" prop_HeaderLength,
        testProperty "Metadata length" prop_MetadataLength,
--        testProperty "Blocks length" prop_BlocksLength, --
        testProperty "Block Index get & put inverses" prop_BlockIndexGetAndPutAreInverses,
        testProperty "Metadata get and put inverses" prop_MetadataGetAndPutAreInverses,
        testProperty "Header get & put inverses" prop_HeaderGetAndPutAreInverses,
        testProperty "Value get & put inverses" prop_ValueGetAndPutAreInverses,
        testProperty "Blocks get & put inverses" prop_BlocksGetAndPutAreInverses,
        testProperty "Yxdb get & put inverses" prop_YxdbFileGetAndPutAreInverses,
        testCase "Loading small module" test_LoadingSmallModule
    ]
