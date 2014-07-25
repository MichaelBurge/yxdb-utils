module Tests.Database.Alteryx (yxdbTests) where

import Database.Alteryx (Header(..))

import Prelude hiding (readFile)

import Data.Binary
import Data.ByteString
import System.IO.Unsafe (unsafePerformIO)
import Test.Framework
import Test.Framework.Providers.HUnit (testCase)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck

instance Arbitrary ByteString where
  arbitrary = fmap pack arbitrary

instance Arbitrary Header where
  arbitrary = do
    fDescription <- arbitrary
    fFileId <- arbitrary
    fCreationDate <- arbitrary
    fFlags1 <- arbitrary
    fFlags2 <- arbitrary
    fMetaInfoLength <- arbitrary
    fSpatialIndexPos <- arbitrary
    fRecordBlockIndexPos <- arbitrary
    fCompressionVersion <- arbitrary
    fMetaInfoXml <- arbitrary
    return $ Header {
      description = fDescription,
      fileId = fFileId,
      creationDate = fCreationDate,
      flags1 = fFlags1,
      flags2 = fFlags2,
      metaInfoLength = fMetaInfoLength,
      spatialIndexPos = fSpatialIndexPos,
      recordBlockIndexPos = fRecordBlockIndexPos,
      compressionVersion = fCompressionVersion,
      metaInfoXml = fMetaInfoXml
    }

exampleFilename :: String
exampleFilename = "small_module.yxdb"

exampleContents :: ByteString
exampleContents = unsafePerformIO $ readFile exampleFilename

prop_getAndPutAreInverses :: Header -> Bool
prop_getAndPutAreInverses x = (decode . encode) x == id x

-- test_parseHeader = 

yxdbTests =
    testGroup "YXDB" [
        testProperty "get & put inverses" prop_getAndPutAreInverses
--        testCase "Parsing header" test_parseHeader
    ]
