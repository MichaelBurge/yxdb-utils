{-# LANGUAGE OverloadedStrings,MultiParamTypeClasses #-}

module Database.Alteryx.Fields
       (
         FieldValue(..),
         FieldType(..),
         Field(..),
         getValue,
         parseFieldType,
         putValue,
         renderFieldType
       ) where

import Data.Bimap as Bimap (Bimap(..), fromList, lookup, lookupR)
import Data.Binary
import Data.Binary.C()
import Data.Decimal (Decimal(..))
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Int
import Data.Text as T
import Data.Time
import Foreign.C.Types

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
      fieldName  :: Text,
      fieldType  :: FieldType,
      fieldSize  :: Maybe Int,
      fieldScale :: Maybe Int
} deriving (Eq, Show)


fieldTypeMap :: Bimap FieldType Text
fieldTypeMap =
    Bimap.fromList
    [
     (FTBool,          "Bool"),
     (FTByte,          "Byte"),
     (FTInt16,         "Int16"),
     (FTInt32,         "Int32"),
     (FTInt64,         "Int64"),
     (FTFixedDecimal,  "FixedDecimal"),
     (FTFloat,         "Float"),
     (FTDouble,        "Double"),
     (FTString,        "String"),
     (FTWString,       "WString"),
     (FTVString,       "V_String"),
     (FTVWString,      "V_WString"),
     (FTDate,          "Date"),
     (FTTime,          "Time"),
     (FTDateTime,      "DateTime"),
     (FTBlob,          "Blob"),
     (FTSpatialObject, "SpatialObj"),
     (FTUnknown,       "Unknown")
    ]
    
putValue :: FieldValue -> Put
putValue value = do
  case value of
    FVBool x          -> error "putBool unimplemented"
    FVByte x          -> error "putByte unimplemented"
    FVInt16 x         -> error "putInt16 unimplemented"
    FVInt32 x         -> error "putInt32 unimplemented"
    FVInt64 x         -> error "putInt64 unimplemented"
    FVFixedDecimal x  -> error "putFixedDecimal unimplemented"
    FVFloat x         -> error "putFloat unimplemented"
    FVDouble x        -> do
      let y = realToFrac x :: CDouble
      put y
      putWord8 0
    FVString x        -> error "putString unimplemented"
    FVWString x       -> error "putWString unimplemented"
    FVVString x       -> error "putVString unimplemented"
    FVVWString x      -> error "putVWString unimplemented"
    FVDate x          -> error "putDate unimplemented"
    FVTime x          -> error "putTime unimplemented"
    FVDateTime x      -> error "putDateTime unimplemented"
    FVBlob x          -> error "putBlob unimplemented"
    FVSpatialObject x -> error "putSpatialObject unimplemented"
    FVUnknown       -> error "putUnknown unimplemented"

getValue :: Field -> Get FieldValue
getValue field =
    case fieldType field of
      FTBool          -> error "getBool unimplemented"
      FTByte          -> error "getByte unimplemented"
      FTInt16         -> error "getInt16 unimplemented"
      FTInt32         -> error "getInt32 unimplemented"
      FTInt64         -> error "getInt64 unimplemented"
      FTFixedDecimal  -> error "getFixedDecimal unimplemented"
      FTFloat         -> error "getFloat unimplemented"
      FTDouble        -> do
        double <- get :: Get CDouble
        _ <- getWord8
        return $ FVDouble $ realToFrac double
      FTString        -> error "getString unimplemented"
      FTWString       -> error "getWString unimplemented"
      FTVString       -> error "getVString unimplemented"
      FTVWString      -> error "getVWString unimplemented"
      FTDate          -> error "getDate unimplemented"
      FTTime          -> error "getTime unimplemented"
      FTDateTime      -> error "getDateTime unimplemented"
      FTBlob          -> error "getBlob unimplemented"
      FTSpatialObject -> error "getSpatialObject unimplemented"
      FTUnknown       -> error "getUnknown unimplemented"

parseFieldType :: Text -> FieldType
parseFieldType text =
    case Bimap.lookupR text fieldTypeMap of
      Nothing -> FTUnknown
      Just x -> x

renderFieldType :: FieldType -> Text
renderFieldType fieldType =
    case Bimap.lookup fieldType fieldTypeMap of
      Nothing -> error $ "No field type assigned to " ++ show fieldType
      Just x -> x
