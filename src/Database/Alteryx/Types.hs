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

data DbType = WrigleyDb | WrigleyDb_NoSpatialIndex deriving (Eq, Show)

data YxdbFile = YxdbFile {
      _header     :: Header,
      _metadata   :: RecordInfo,
      _records    :: [Record],
      _blockIndex :: BlockIndex
} deriving (Eq, Show)

data Header = Header {
      _description         :: BS.ByteString, -- 64 bytes
      _fileId              :: Word32,
      _creationDate        :: Word32, -- TODO: Confirm whether this is UTC or user's local time
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

newtype Record = Record [ Maybe FieldValue ] deriving (Eq, Show)
newtype RecordInfo = RecordInfo [ Field ] deriving (Eq, Show)
newtype Blocks = Blocks BSL.ByteString deriving (Eq, Show)
newtype BlockIndex = BlockIndex (UArray Int Int64) deriving (Eq, Show)

data FieldValue = FVBool Bool
                | FVByte Int8
                | FVInt16 Int16
                | FVInt32 Int32
                | FVInt64 Int64
                | FVFixedDecimal Decimal
                | FVFloat Float
                | FVDouble Double
                | FVString Text
                | FVWString Text
                | FVVString Text
                | FVVWString Text
                | FVDate Day
                | FVTime DiffTime
                | FVDateTime UTCTime
                | FVBlob BSL.ByteString
                | FVSpatialObject BSL.ByteString
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

instance NT.Newtype Record [Maybe FieldValue] where
    pack = Record
    unpack (Record xs) = xs

instance NT.Newtype Blocks BSL.ByteString where
    pack = Blocks
    unpack (Blocks x) = x

instance NT.Newtype RecordInfo [Field] where
    pack = RecordInfo
    unpack (RecordInfo x) = x

makeLenses ''Field
makeLenses ''YxdbFile
makeLenses ''Header
