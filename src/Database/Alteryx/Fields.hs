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
import Control.Monad
import Data.Bimap as Bimap (Bimap, fromList, lookup, lookupR)
import Data.Binary
import Data.Binary.C()
import Data.Binary.Get
import Data.Binary.Put
import Data.Bits
import Data.Decimal (Decimal)
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
    Just (FVInt16 x)         -> do
      putWord16le $ fromIntegral x
      putWord8 0
    Just (FVInt32 x)         -> do
      putWord32le $ fromIntegral x
      putWord8 0
    Just (FVInt64 x)         -> error "putInt64 unimplemented"
    Just (FVFixedDecimal x)  -> error "putFixedDecimal unimplemented"
    Just (FVFloat x)         -> error "putFloat unimplemented"
    Just (FVDouble x)        -> do
           let y = realToFrac x :: CDouble
           put y
           putWord8 0
    Just (FVString x)        -> do
           let stringBS = encodeUtf8 x -- TODO: This should actually be Latin-1 to match the getter
           let numPaddingBytes = case field ^. fieldSize of
                                   Nothing -> error "putValue: No size given for string value"
                                   Just x  -> x - BS.length stringBS + 1
           putByteString $ encodeUtf8 $ x
           replicateM_ numPaddingBytes $ putWord8 0

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
    Nothing ->
      case field ^. fieldType of
        FTDouble -> do
          put (0 :: CDouble)
          putWord8 1
        x -> error $ "putValue unimplemented for null values of type " ++ show x

getValue :: Field -> Get (Maybe FieldValue)
getValue field =
    let getFixedStringWithSize :: Maybe Int -> Int -> (BS.ByteString -> Text) -> Get (Maybe Text)
        getFixedStringWithSize size charBytes decoder =
          let mNumBytes = (charBytes*) <$> size
          in case mNumBytes of
            Just numBytes -> do
              bs <- getByteString numBytes
              isNull <- getWord8
              return $ if isNull > 0
                       then Nothing
                       else Just $ decoder bs
            Nothing -> error "getValue: String field had no size"
        getFixedString :: Int -> (BS.ByteString -> Text) -> Get (Maybe Text)
        getFixedString = getFixedStringWithSize $ field ^. fieldSize
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
        getWithNullByte :: Get a -> Get (Maybe a)
        getWithNullByte getter = do
          x <- getter
          isNull <- getWord8
          return $ if isNull > 0
                   then Nothing
                   else Just x
    in case field ^. fieldType of
         FTBool          -> error "getBool unimplemented"
         FTByte          -> fmap (FVByte  . fromIntegral) <$> getWithNullByte getWord8
         FTInt16         -> fmap (FVInt16 . fromIntegral) <$> getWithNullByte getWord16le
         FTInt32         -> fmap (FVInt32 . fromIntegral) <$> getWithNullByte getWord32le
         FTInt64         -> fmap (FVInt64 . fromIntegral) <$> getWithNullByte getWord64le
         FTFixedDecimal  -> fmap FVString <$> getFixedString 1 decodeLatin1 -- TODO: WRONG!
         FTFloat         -> fmap (FVFloat . realToFrac) <$> getWithNullByte (get :: Get CFloat)
         FTDouble        -> fmap (FVDouble . realToFrac) <$> getWithNullByte (get :: Get CDouble)
         FTString        -> fmap FVString <$> getFixedString 1 decodeLatin1
         FTWString       -> fmap FVWString <$> getFixedString 2 decodeUtf16LE
         FTVString       -> fmap FVVString <$> getVarString
         FTVWString      -> fmap FVVWString <$> getVarString
         FTDate          -> fmap FVString <$> getFixedStringWithSize (Just 10) 1 decodeLatin1 -- TODO: WRONG!
         FTTime          -> fmap FVString <$> getFixedStringWithSize (Just 8) 1 decodeLatin1 -- TODO: WRONG!
         FTDateTime      -> fmap FVString <$> getFixedStringWithSize (Just 19) 1 decodeLatin1 -- TODO: WRONG!
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
