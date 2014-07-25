module Tests.Database.Alteryx (yxdbTests) where

import Database.Alteryx (Header(..), YxdbFile(..))

import Prelude hiding (readFile)

import Data.Binary
import Data.ByteString
import System.IO.Unsafe (unsafePerformIO)
import Test.Framework
import Test.Framework.Providers.HUnit (testCase)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck
import Test.QuickCheck.Instances

-- instance Arbitrary ByteString where
--   arbitrary = fmap pack arbitrary

instance Arbitrary YxdbFile where
  arbitrary = do
    fCopyright <- arbitrary
    fHeader <- arbitrary
    fContents <- arbitrary

    return $ YxdbFile {
      copyright = fCopyright,
      header    = fHeader,
      contents  = fContents
    }

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

prop_HeaderGetAndPutAreInverses :: Header -> Bool
prop_HeaderGetAndPutAreInverses x = (decode . encode) x == x

prop_YxdbFileGetAndPutAreInverses :: YxdbFile -> Bool
prop_YxdbFileGetAndPutAreInverses x = (decode . encode) x == x

yxdbTests =
    testGroup "YXDB" [
        testProperty "Header get & put inverses" prop_HeaderGetAndPutAreInverses,
        testProperty "Yxdb get & put inverses" prop_YxdbFileGetAndPutAreInverses
    ]
