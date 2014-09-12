module Tests.Database.Alteryx (yxdbTests) where

import Database.Alteryx

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
import Test.QuickCheck.Property

import System.Directory
import System.IO

instance Arbitrary YxdbFile where
  arbitrary = do
    fMetadata   <- arbitrary
    blockSize   <- choose(4,1000)
    fBlocks     <- resize blockSize $ arbitraryBlocksMatching fMetadata
    fHeader     <- arbitraryHeaderMatching fMetadata fBlocks
    fBlockIndex <- arbitrary

    fRecords <- arbitraryRecordsMatching fMetadata

    return $ YxdbFile {
      _header     = fHeader,
      _records    = fRecords,
      _metadata   = fMetadata,
      _blockIndex = fBlockIndex
    }

arbitraryBlocksMatching :: RecordInfo -> Gen Blocks
arbitraryBlocksMatching metadata =
    sized $ \blockSize -> do
      when (blockSize < 4) $ fail $ "Invalid block size" ++ show blockSize
      blocksBS <- vector $ blockSize - 4

      return $ Blocks $ BSL.pack blocksBS

arbitraryHeaderMatching :: RecordInfo -> Blocks -> Gen Header
arbitraryHeaderMatching metadata blocks = do
  fDescription <- vector 64 :: Gen [Word8]
  fFileId <- arbitrary
  fCreationDate <- arbitrary
  fFlags1 <- arbitrary
  fFlags2 <- arbitrary
  fMystery <- arbitrary
  fSpatialIndexPos <- arbitrary
  let numMetadataBytes = numMetadataBytesActual metadata
  let fMetaInfoLength = fromIntegral $ numMetadataBytes `div` 2
  let numBlocksBytes = numBlocksBytesActual blocks
  let startOfBlocks = fromIntegral $ headerPageSize + (fromIntegral $ numMetadataBytes)
  let fRecordBlockIndexPos = startOfBlocks + (fromIntegral numBlocksBytes)
  fNumRecords <- arbitrary
  fCompressionVersion <- arbitrary
  fReservedSpace <- vector (512 - 64 - (4 * 7) - (8 * 3)) :: Gen [Word8]
  return $ Header {
               _description         = BS.pack fDescription,
               _fileId              = fFileId,
               _creationDate        = fCreationDate,
               _flags1              = fFlags1,
               _flags2              = fFlags2,
               _metaInfoLength      = fMetaInfoLength,
               _mystery             = fMystery,
               _spatialIndexPos     = fSpatialIndexPos,
               _recordBlockIndexPos = fRecordBlockIndexPos,
               _numRecords          = fNumRecords,
               _compressionVersion  = fCompressionVersion,
               _reservedSpace       = BS.pack fReservedSpace
             }

instance Arbitrary Header where
    arbitrary = do
      metadata <- arbitrary
      blocks <- arbitraryBlocksMatching metadata
      arbitraryHeaderMatching metadata blocks

arbitraryRecordsMatching :: RecordInfo -> Gen [Record]
arbitraryRecordsMatching metadata = do
  size <- choose(0,10000)
  vectorOf size (arbitraryRecordMatching metadata)

arbitraryRecordMatching :: RecordInfo -> Gen Record
arbitraryRecordMatching (RecordInfo fields) =
    Record <$> mapM arbitraryValueMatching fields

arbitraryValueMatching :: Field -> Gen (Maybe FieldValue)
arbitraryValueMatching field =
  let value =
        case field ^. fieldType of
          FTDouble -> FVDouble <$> arbitrary
  in oneof [
    return Nothing,
    Just <$> value
    ]

instance Arbitrary Blocks where
    arbitrary = do
      metadata <- arbitrary
      arbitraryBlocksMatching metadata

instance Arbitrary FieldType where
    arbitrary = elements [
                 -- FTBool,
                 -- FTByte,
                 -- FTInt16,
                 -- FTInt32,
                 -- FTInt64,
                 -- FTFixedDecimal,
                 -- FTFloat,
                 FTDouble
                 -- FTString,
                 -- FTWString,
                 -- FTVString,
                 -- FTVWString,
                 -- FTDate,
                 -- FTTime,
                 -- FTDateTime,
                 -- FTBlob,
                 -- FTSpatialObject,
                 -- FTUnknown
                ]

instance Arbitrary Field where
    arbitrary = do
      fName <- arbitrary
      fType <- arbitrary
      fSize <- arbitrary
      fScale <- arbitrary
      return $ Field {
                   _fieldName = fName,
                   _fieldType = fType,
                   _fieldSize = fSize,
                   _fieldScale = fScale
                 }

instance Arbitrary Record where
    arbitrary = arbitraryRecordMatching =<< arbitrary

instance Arbitrary RecordInfo where
    arbitrary = do
      len <- choose(1,10)
      RecordInfo <$> vector len

instance Arbitrary BlockIndex where
    arbitrary =
        sized $
            \chunkSize -> do
                indices <- replicateM chunkSize arbitrary
                return $ BlockIndex $ listArray (0, chunkSize) indices

data PairedValue = PairedValue Field (Maybe FieldValue) deriving (Eq, Show)

instance Arbitrary PairedValue where
    arbitrary = do
      field <- arbitrary
      value <- arbitraryValueMatching field
      return $ PairedValue field value

exampleFilename :: String
exampleFilename = "small-module.yxdb"

exampleBlocks :: BSL.ByteString
exampleBlocks = unsafePerformIO $ BSL.readFile exampleFilename

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
    assertEq (numMetadataBytesHeader $ yxdb ^. header) (numMetadataBytesActual $ yxdb ^. metadata)

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

prop_BlocksGetAndPutAreInverses :: Blocks -> Property
prop_BlocksGetAndPutAreInverses x = assertEq (decode $ encode x) x

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
--        testProperty "Blocks length" prop_BlocksLength, --
        testProperty "Block Index get & put inverses" prop_BlockIndexGetAndPutAreInverses,
        testProperty "Metadata get and put inverses" prop_MetadataGetAndPutAreInverses,
        testProperty "Header get & put inverses" prop_HeaderGetAndPutAreInverses,
        testProperty "Value get & put inverses" prop_ValueGetAndPutAreInverses,
        testProperty "Blocks get & put inverses" prop_BlocksGetAndPutAreInverses,
        testProperty "Yxdb get & put inverses" prop_YxdbFileGetAndPutAreInverses,
        testCase "Loading small module" test_LoadingSmallModule
    ]
