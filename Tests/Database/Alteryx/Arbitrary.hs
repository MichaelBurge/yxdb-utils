{-# OPTIONS_GHC -fno-warn-orphans #-}

module Tests.Database.Alteryx.Arbitrary where

import Database.Alteryx

import Conduit
import Data.Conduit.Lift as CL
import Control.Applicative
import Control.Lens hiding (elements)
import Control.Monad
import Data.Array.IArray
import Data.Binary
import Data.Binary.Put
import Data.ByteString as BS
import Data.ByteString.Lazy as BSL
import Data.Maybe
import qualified Data.Text as T
import Data.Text.Encoding
import Data.Time.Clock.POSIX
import Data.Word
import Test.QuickCheck
import Test.QuickCheck.Instances()

instance Arbitrary YxdbFile where
  arbitrary = do
    fMetadata   <- arbitrary
    fBlocks     <- arbitraryBlocksMatching fMetadata
    fHeader     <- arbitraryHeaderMatching fMetadata fBlocks
    fBlockIndex <- arbitrary

    fRecords <- arbitraryRecordsMatching fMetadata

    return $ YxdbFile {
      _yxdbFileHeader     = fHeader,
      _yxdbFileRecords    = fRecords,
      _yxdbFileMetadata   = fMetadata,
      _yxdbFileBlockIndex = fBlockIndex
    }

arbitraryBlocksMatching :: RecordInfo -> Gen [Block]
arbitraryBlocksMatching recordInfo = do
  records <- arbitraryRecordsMatching recordInfo
  let blocks = fromJust $ yieldMany records $= CL.evalStateLC defaultStatistics (recordsToBlocks recordInfo) $$ sinkList
  return blocks

arbitraryHeaderMatching :: RecordInfo -> [Block] -> Gen Header
arbitraryHeaderMatching metadata blocks = do
  fDescription <- replicateM 64 $ choose(0,127) :: Gen [Word8]
  fFileId <- arbitrary
  fCreationDate <- posixSecondsToUTCTime <$> fromIntegral <$> (arbitrary :: Gen Word32)
  fFlags1 <- arbitrary
  fFlags2 <- arbitrary
  fMystery <- arbitrary
  fSpatialIndexPos <- arbitrary
  let numMetadataBytes = numMetadataBytesActual metadata
  let fMetaInfoLength = fromIntegral $ numMetadataBytes `div` 2
  let numBlockBytes = sum $ Prelude.map numBlockBytesActual blocks
  let startOfBlocks = fromIntegral $ headerPageSize + (fromIntegral $ numMetadataBytes)
  let fRecordBlockIndexPos = startOfBlocks + (fromIntegral numBlockBytes)
  fNumRecords <- arbitrary
  fCompressionVersion <- arbitrary
  fReservedSpace <- vector (512 - 64 - (4 * 7) - (8 * 3)) :: Gen [Word8]
  return $ Header {
               _description         = decodeUtf8 $ BS.pack $ fDescription,
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
  in do
    isNull <- arbitrary
    if isNull
       then return Nothing
       else Just <$> value

instance Arbitrary Block where
    arbitrary = Prelude.head <$> (arbitraryBlocksMatching =<< arbitrary)

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
