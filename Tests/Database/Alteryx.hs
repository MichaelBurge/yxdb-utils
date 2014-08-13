module Tests.Database.Alteryx (yxdbTests) where

import Database.Alteryx
  (
    numReservedSpaceBytes,
    Header(..),
    YxdbFile(..)
  )

import Prelude hiding (readFile)

import Data.Binary
import Data.ByteString.Lazy as BS
import Data.Encoding (decodeLazyByteString, Encoding)
import Data.Encoding.UTF8
import Data.Text.Lazy as T
import Data.Text.Lazy.Encoding
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

    return $ YxdbFile {
      header    = fHeader,
      contents  = fContents
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
    fReservedSpace <- vector numReservedSpaceBytes
    metaInfoXmlBS <- vectorOf (fromIntegral $ fMetaInfoLength) (choose(0, 127))
    let fMetaInfoXml = T.pack $ decodeLazyByteString UTF8 $ BS.pack $ metaInfoXmlBS

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
            reservedSpace       = BS.pack fReservedSpace,
            metaInfoXml         = fMetaInfoXml
    }

exampleFilename :: String
exampleFilename = "small_module.yxdb"

exampleContents :: ByteString
exampleContents = unsafePerformIO $ readFile exampleFilename

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
