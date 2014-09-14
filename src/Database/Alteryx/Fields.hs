{-# LANGUAGE OverloadedStrings #-}

module Database.Alteryx.Fields
       (
         getValue,
         parseFieldType,
         putValue,
         renderFieldType
       ) where

import Control.Applicative
import Control.Lens
import Data.Bimap as Bimap (Bimap(..), fromList, lookup, lookupR)
import Data.Binary
import Data.Binary.C()
import Data.Binary.Get
import Data.Binary.Put
import Data.Bits
import Data.Decimal (Decimal(..))
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Int
import Data.Text as T
import Data.Text.Encoding
import Data.Time
import Foreign.C.Types

import Database.Alteryx.Types

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

putValue :: Field -> Maybe FieldValue -> Put
putValue field value = do
  case value of
    Just (FVBool x)          -> error "putBool unimplemented"
    Just (FVByte x)          -> error "putByte unimplemented"
    Just (FVInt16 x)         -> error "putInt16 unimplemented"
    Just (FVInt32 x)         -> error "putInt32 unimplemented"
    Just (FVInt64 x)         -> error "putInt64 unimplemented"
    Just (FVFixedDecimal x)  -> error "putFixedDecimal unimplemented"
    Just (FVFloat x)         -> error "putFloat unimplemented"
    Just (FVDouble x)        -> do
           let y = realToFrac x :: CDouble
           put y
           putWord8 0
    Just (FVString x)        -> do
           putByteString $ encodeUtf16LE $ x
           putWord16le 0
    Just (FVWString x)       -> do
           putByteString $ encodeUtf16LE $ x
           putWord16le 0
    Just (FVVString x)       -> error "putVString unimplemented"
    Just (FVVWString x)      -> error "putVWString unimplemented"
    Just (FVDate x)          -> error "putDate unimplemented"
    Just (FVTime x)          -> error "putTime unimplemented"
    Just (FVDateTime x)      -> error "putDateTime unimplemented"
    Just (FVBlob x)          -> error "putBlob unimplemented"
    Just (FVSpatialObject x) -> error "putSpatialObject unimplemented"
    Just (FVUnknown)         -> error "putUnknown unimplemented"
    Nothing -> error "putValue unimplemented for null values"

getValue :: Field -> Get (Maybe FieldValue)
getValue field =
    let getFixedString :: Int -> (BS.ByteString -> Text) -> Get (Maybe Text)
        getFixedString charBytes decoder =
          let mNumBytes = (charBytes*) <$> field ^. fieldSize
          in case mNumBytes of
            Just numBytes -> do
              bs <- getByteString numBytes
              isNull <- getWord8
              return $ if isNull > 0
                       then Nothing
                       else Just $ decoder bs
            Nothing -> error "getValue: String field had no size"
        getVarString :: Get (Maybe Text)
        getVarString = do
          initialLength <- getWord32le
          case initialLength of
            0 -> return $ Just ""
            1 -> return $ Nothing
            _ -> let lengthInBytes = fromIntegral $ initialLength `shiftR` 28
                 in do
                   bs <- getByteString lengthInBytes
                   return $ Just $ decodeUtf16LE bs
    in case field ^. fieldType of
         FTBool          -> error "getBool unimplemented"
         FTByte          -> error "getByte unimplemented"
         FTInt16         -> do
           int <- fromIntegral <$> getWord16le :: Get Int16
           _ <- getWord8
           return $ Just $ FVInt16 int
         FTInt32         -> do
           int <- fromIntegral <$> getWord32le :: Get Int32
           _ <- getWord8
           return $ Just $ FVInt32 int
         FTInt64         -> error "getInt64 unimplemented"
         FTFixedDecimal  -> error "getFixedDecimal unimplemented"
         FTFloat         -> error "getFloat unimplemented"
         FTDouble        -> do
           double <- get :: Get CDouble
           _ <- getWord8
           return $ Just $ FVDouble $ realToFrac double
         FTString        -> (FVString <$>) <$> getFixedString 1 decodeLatin1
         FTWString       -> (FVWString <$>) <$> getFixedString 2 decodeUtf16LE
         FTVString       -> (FVVString <$>) <$> getVarString
         FTVWString      -> (FVVWString <$>) <$> getVarString
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
