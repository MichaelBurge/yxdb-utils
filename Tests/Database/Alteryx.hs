module Tests.Database.Alteryx (yxdbTests) where

import Database.Alteryx
  (
    Header(..),
    Content(..),
    Metadata(..),
    YxdbFile(..)
  )

import Prelude hiding (readFile)

import Data.Binary
import Data.ByteString as BS
import Data.ByteString.Lazy as BSL
import Data.Encoding (decodeLazyByteString, Encoding)
import Data.Encoding.UTF8
import Data.Text as T
import Data.Text.Encoding
import System.IO.Unsafe (unsafePerformIO)
import Test.Framework
import Test.Framework.Providers.HUnit (testCase)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck
import Test.QuickCheck.Instances

instance Arbitrary YxdbFile where
  arbitrary = do
    fHeader <- arbitrary
    fContents <- arbitrary

    metaInfoXmlBS <- vectorOf (fromIntegral $ metaInfoLength fHeader) (choose(0, 127))
    let fMetaInfoXml = Metadata $ T.pack $ decodeLazyByteString UTF8 $ BSL.pack $ metaInfoXmlBS

    return $ YxdbFile {
      header   = fHeader,
      contents = fContents,
      metadata = fMetaInfoXml
    }

instance Arbitrary Header where
  arbitrary = do
    fDescription <- vector 64 :: Gen [Word8]
    fFileId <- arbitrary
    fCreationDate <- arbitrary
    fFlags1 <- arbitrary
    fFlags2 <- arbitrary
    fMetaInfoLength <- choose(0, 1000)
    fSpatialIndexPos <- arbitrary
    fRecordBlockIndexPos <- arbitrary
    fCompressionVersion <- arbitrary
    fReservedSpace <- vector (512 - 64 - (4 * 6) - (8 * 2)) :: Gen [Word8]

    return $ Header {
            description         = BS.pack fDescription,
            fileId              = fFileId,
            creationDate        = fCreationDate,
            flags1              = fFlags1,
            flags2              = fFlags2,
            metaInfoLength      = fMetaInfoLength,
            spatialIndexPos     = fSpatialIndexPos,
            recordBlockIndexPos = fRecordBlockIndexPos,
            compressionVersion  = fCompressionVersion,
            reservedSpace       = BS.pack fReservedSpace
    }

instance Arbitrary Content where
    arbitrary = do
      fContent <- arbitrary
      return $ Content fContent

exampleFilename :: String
exampleFilename = "small_module.yxdb"

exampleContents :: BSL.ByteString
exampleContents = unsafePerformIO $ BSL.readFile exampleFilename

assertEq :: (Eq a, Show a) => a -> a -> Property
assertEq a b = let
    aStr = show a
    bStr = show b
    in printTestCase ("\nA: " ++ aStr ++ "\nB: " ++ bStr) (a == b)

prop_HeaderGetAndPutAreInverses :: Header -> Property
prop_HeaderGetAndPutAreInverses x = assertEq (decode $ encode x) x

prop_YxdbFileGetAndPutAreInverses :: YxdbFile -> Property
prop_YxdbFileGetAndPutAreInverses x = assertEq (decode $ encode x) x

yxdbTests =
    testGroup "YXDB" [
        testProperty "Header get & put inverses" prop_HeaderGetAndPutAreInverses,
        testProperty "Yxdb get & put inverses" prop_YxdbFileGetAndPutAreInverses
    ]
