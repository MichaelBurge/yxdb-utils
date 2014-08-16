module Tests.Database.Alteryx (yxdbTests) where

import Database.Alteryx
  (
    Header(..),
    Content(..),
    Metadata(..),
    YxdbFile(..),
    headerPageSize,
    numContentBytes
  )

import Prelude hiding (readFile)

import Control.Applicative
import Data.Binary
import Data.Binary.Get (runGet)
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
import Test.HUnit (assertFailure)
import Test.HUnit (assertFailure, putTextToHandle)
import Test.QuickCheck
import Test.QuickCheck.Instances

import System.Directory
import System.IO

instance Arbitrary YxdbFile where
  arbitrary = do
    fHeader <- arbitrary
    fContents <- resize (numContentBytes fHeader) arbitrary

    metaInfoXmlBS <- vectorOf (fromIntegral $ metaInfoLength fHeader) (choose(0, 127))
    let fMetaInfoXml = Metadata $ T.pack $ decodeLazyByteString UTF8 $ BSL.pack $ metaInfoXmlBS

    blockIndexLength <- choose (4,100000)
    blockIndexBS <- BSL.pack <$> vector blockIndexLength 
    let fBlockIndex = runGet get blockIndexBS

    return $ YxdbFile {
      header     = fHeader,
      contents   = fContents,
      metadata   = fMetaInfoXml,
      blockIndex = fBlockIndex
    }

instance Arbitrary Header where
  arbitrary = do
    fDescription <- vector 64 :: Gen [Word8]
    fFileId <- arbitrary
    fCreationDate <- arbitrary
    fFlags1 <- arbitrary
    fFlags2 <- arbitrary
    fMetaInfoLength <- choose(0, 1000)
    fMystery <- arbitrary
    fSpatialIndexPos <- arbitrary
    let startOfContent = fromIntegral $ headerPageSize + (fromIntegral $ 2 * fMetaInfoLength)
    fRecordBlockIndexPos <- choose (startOfContent + 4,startOfContent + 1000)
    fNumRecords <- arbitrary
    fCompressionVersion <- arbitrary
    fReservedSpace <- vector (512 - 64 - (4 * 6) - (8 * 2)) :: Gen [Word8]

    return $ Header {
            description         = BS.pack fDescription,
            fileId              = fFileId,
            creationDate        = fCreationDate,
            flags1              = fFlags1,
            flags2              = fFlags2,
            metaInfoLength      = fMetaInfoLength,
            mystery             = fMystery,
            spatialIndexPos     = fSpatialIndexPos,
            recordBlockIndexPos = fRecordBlockIndexPos,
            numRecords          = fNumRecords,
            compressionVersion  = fCompressionVersion,
            reservedSpace       = BS.pack fReservedSpace
    }

instance Arbitrary Content where
    arbitrary =
        sized $ 
            \chunkSize -> do
               contentsBS <- vector $ chunkSize - 4
               return $ Content $ BSL.pack contentsBS

exampleFilename :: String
exampleFilename = "small-module.yxdb"

exampleContents :: BSL.ByteString
exampleContents = unsafePerformIO $ BSL.readFile exampleFilename

assertEq :: (Eq a, Show a) => a -> a -> Property
assertEq a b = let
    aStr = show a
    bStr = show b
    in printTestCase ("\nA: " ++ aStr ++ "\nB: " ++ bStr) (a == b)

prop_HeaderGetAndPutAreInverses :: Header -> Property
prop_HeaderGetAndPutAreInverses x = assertEq (decode $ encode x) x

prop_ContentGetAndPutAreInverses :: Content -> Property
prop_ContentGetAndPutAreInverses x = assertEq (decode $ encode x) x

prop_YxdbFileGetAndPutAreInverses :: YxdbFile -> Property
prop_YxdbFileGetAndPutAreInverses x = assertEq (decode $ encode x) x

test_LoadingSmallModule = do
  parsed <- decodeFileOrFail exampleFilename
  case parsed of
    Left (bytes, msg) -> assertFailure (msg ++ " at " ++ show bytes ++ " bytes")
    Right yxdbFile -> const (return ()) (yxdbFile :: YxdbFile)

yxdbTests =
    testGroup "YXDB" [
        testProperty "Header get & put inverses" prop_HeaderGetAndPutAreInverses,
        testProperty "Content get & put inverses" prop_ContentGetAndPutAreInverses,
        testProperty "Yxdb get & put inverses" prop_YxdbFileGetAndPutAreInverses,
        testCase "Loading small module" test_LoadingSmallModule
    ]
