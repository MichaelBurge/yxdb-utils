{-# LANGUAGE TemplateHaskell,MultiParamTypeClasses,FlexibleInstances #-}

module Database.Alteryx.Types where

import Control.Lens
import qualified Control.Newtype as NT
import Data.Array.Unboxed
import Data.ByteString as BS
import Data.ByteString.Lazy as BSL
import Data.Decimal
import Data.Int
import Data.Text as T
import Data.Time
import Data.Word
import qualified Data.Vector as V

data DbType = WrigleyDb | WrigleyDb_NoSpatialIndex | CalgaryDb deriving (Eq, Show)

data YxdbMetadata = YxdbMetadata {
  _metadataHeader     :: Header,
  _metadataRecordInfo :: RecordInfo,
  _metadataBlockIndex :: BlockIndex
  } deriving (Eq, Show)

data YxdbFile = YxdbFile {
  _yxdbFileHeader     :: Header,
  _yxdbFileMetadata   :: RecordInfo,
  _yxdbFileRecords    :: [Record],
  _yxdbFileBlockIndex :: BlockIndex
} deriving (Eq, Show)

data CalgaryFile = CalgaryFile {
      _calgaryFileHeader   :: CalgaryHeader,
      _calgaryFileMetadata :: CalgaryRecordInfo,
      _calgaryFileRecords  :: [V.Vector Record],
      _calgaryFileIndex    :: BlockIndex
    } deriving (Eq, Show)

data Header = Header {
      _description         :: Text,
      _fileId              :: Word32,
      _creationDate        :: UTCTime,
      _flags1              :: Word32,
      _flags2              :: Word32,
      _metaInfoLength      :: Word32,
      _mystery             :: Word32,
      _spatialIndexPos     :: Word64,
      _recordBlockIndexPos :: Word64,
      _numRecords          :: Word64,
      _compressionVersion  :: Word32,
      _reservedSpace       :: BS.ByteString
} deriving (Eq, Show)

data CalgaryHeader = CalgaryHeader {
      _calgaryHeaderDescription :: Text,
      _calgaryHeaderFileId :: Word32,
      _calgaryHeaderCreationDate :: UTCTime,
      _calgaryHeaderFlags1 :: Word32,
      _calgaryHeaderFlags2 :: Word32,
      _calgaryHeaderNumRecords :: Word32,
      _calgaryHeaderMystery1 :: Word32,
      _calgaryHeaderMystery2 :: Word32,
      _calgaryHeaderMystery3 :: Word32,
      _calgaryHeaderMystery4 :: Word32,
      _calgaryHeaderReserved :: BS.ByteString
    } deriving (Eq, Show)


newtype Record = Record [ Maybe FieldValue ] deriving (Eq, Show)
newtype RecordInfo = RecordInfo [ Field ] deriving (Eq, Show)
newtype Miniblock = Miniblock BS.ByteString deriving (Eq, Show)
newtype Block = Block BSL.ByteString deriving (Eq, Show)
newtype BlockIndex = BlockIndex (UArray Int Int64) deriving (Eq, Show)
newtype CalgaryRecordInfo = CalgaryRecordInfo RecordInfo deriving (Eq, Show)

data FieldValue = FVBool !Bool
                | FVByte !Int8
                | FVInt16 !Int16
                | FVInt32 !Int32
                | FVInt64 !Int64
                | FVFixedDecimal !Decimal
                | FVFloat !Float
                | FVDouble !Double
                | FVString !Text
                | FVWString !Text
                | FVVString !Text
                | FVVWString !Text
                | FVDate !Day
                | FVTime !DiffTime
                | FVDateTime !UTCTime
                | FVBlob !BSL.ByteString
                | FVSpatialObject !BSL.ByteString
                | FVUnknown
                deriving (Eq, Show)

data FieldType = FTBool
               | FTByte
               | FTInt16
               | FTInt32
               | FTInt64
               | FTFixedDecimal
               | FTFloat
               | FTDouble
               | FTString
               | FTWString
               | FTVString
               | FTVWString
               | FTDate -- yyyy-mm-dd
               | FTTime -- hh:mm:ss
               | FTDateTime -- yyyy-mm-dd hh:mm:ss
               | FTBlob
               | FTSpatialObject
               | FTUnknown
               deriving (Eq, Ord, Show)

data Field = Field {
      _fieldName  :: Text,
      _fieldType  :: FieldType,
      _fieldSize  :: Maybe Int,
      _fieldScale :: Maybe Int
} deriving (Eq, Show)

data StreamingCSVStatistics = StreamingCSVStatistics {
      _statisticsMetadataLength :: Int,
      _statisticsNumRecords     :: Int,
      _statisticsBlockLengths   :: [Int]
    } deriving (Eq, Show)

defaultStatistics :: StreamingCSVStatistics
defaultStatistics = StreamingCSVStatistics {
                      _statisticsMetadataLength = error "No metadata length",
                      _statisticsNumRecords     = 0,
                      _statisticsBlockLengths   = []
                    }

instance NT.Newtype Record [Maybe FieldValue] where
  pack = Record
  unpack (Record xs) = xs

instance NT.Newtype Block BSL.ByteString where
  pack = Block
  unpack (Block x) = x

instance NT.Newtype Miniblock BS.ByteString where
  pack = Miniblock
  unpack (Miniblock x) = x

instance NT.Newtype RecordInfo [Field] where
  pack = RecordInfo
  unpack (RecordInfo x) = x

instance NT.Newtype BlockIndex (UArray Int Int64) where
  pack = BlockIndex
  unpack (BlockIndex x) = x

makeLenses ''Field
makeLenses ''YxdbFile
makeLenses ''Header
makeLenses ''YxdbMetadata
makeLenses ''StreamingCSVStatistics
makeLenses ''CalgaryHeader
