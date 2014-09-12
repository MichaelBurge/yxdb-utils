{-# OPTIONS_GHC -fno-warn-orphans #-}

module Tests.Database.Alteryx.Arbitrary where

import Database.Alteryx

import Control.Applicative
import Control.Lens hiding (elements)
import Control.Monad
import Data.Array.IArray
import Data.ByteString as BS
import Data.ByteString.Lazy as BSL
import Data.Word
import Test.QuickCheck

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
      blocksBS <- vector $ blockSize - 4 :: Gen [Word8]
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
    arbitrary = arbitraryBlocksMatching =<< arbitrary

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
