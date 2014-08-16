module Tests.Database.Alteryx (yxdbTests) where

import Database.Alteryx
  (
    BlockIndex(..),
    Header(..),
    Content(..),
    Metadata(..),
    YxdbFile(..),
    headerPageSize,
    numContentBytes,
    numMetadataBytes
  )

import Prelude hiding (readFile)

import Control.Applicative
import Control.Monad (replicateM)
import Data.Array.IArray (listArray)
import Data.Binary
import Data.Binary.Get (runGet)
import Data.Binary.Put (runPut)
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
    fMetadata <- resize (fromIntegral $ metaInfoLength fHeader) arbitrary
    fBlockIndex <- arbitrary

    return $ YxdbFile {
      header     = fHeader,
      content    = fContents,
      metadata   = fMetadata,
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
    fRecordBlockIndexPos <- choose (startOfContent + 4, startOfContent + 1000)
    fNumRecords <- arbitrary
    fCompressionVersion <- arbitrary
    fReservedSpace <- vector (512 - 64 - (4 * 7) - (8 * 3)) :: Gen [Word8]

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

instance Arbitrary Metadata where
    arbitrary =
        sized $
              \size -> do
                  bs <- vectorOf size (choose(0, 127))
                  return $ Metadata $ T.pack $ decodeLazyByteString UTF8 $ BSL.pack $ bs
                  

instance Arbitrary Content where
    arbitrary =
        sized $ 
            \chunkSize -> do
               contentsBS <- vector $ chunkSize - 4
               return $ Content $ BSL.pack contentsBS

instance Arbitrary BlockIndex where
    arbitrary =
        sized $
            \chunkSize -> do
                indices <- replicateM chunkSize arbitrary
                return $ BlockIndex $ listArray (0, chunkSize) indices

exampleFilename :: String
exampleFilename = "small-module.yxdb"

exampleContents :: BSL.ByteString
exampleContents = unsafePerformIO $ BSL.readFile exampleFilename

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
    assertEq (numMetadataBytes $ header yxdb) (numBytes $ metadata yxdb)

prop_ContentLength :: YxdbFile -> Property
prop_ContentLength yxdb =
    assertEq (numContentBytes $ header yxdb) (numBytes $ content yxdb)

prop_BlockIndexGetAndPutAreInverses :: BlockIndex -> Property
prop_BlockIndexGetAndPutAreInverses x = assertEq (decode $ encode x) x

prop_MetadataGetAndPutAreInverses :: Metadata -> Property
prop_MetadataGetAndPutAreInverses x = assertEq (decode $ encode x) x

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
        testProperty "Header length" prop_HeaderLength,
        testProperty "Metadata length" prop_MetadataLength,
        testProperty "Content length" prop_ContentLength,
        testProperty "Block Index get & put inverses" prop_BlockIndexGetAndPutAreInverses,
        testProperty "Metadata get and put inverses" prop_MetadataGetAndPutAreInverses,
        testProperty "Header get & put inverses" prop_HeaderGetAndPutAreInverses,
        testProperty "Content get & put inverses" prop_ContentGetAndPutAreInverses,
        testProperty "Yxdb get & put inverses" prop_YxdbFileGetAndPutAreInverses,
        testCase "Loading small module" test_LoadingSmallModule
    ]
