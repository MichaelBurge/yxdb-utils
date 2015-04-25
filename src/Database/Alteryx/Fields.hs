{-# LANGUAGE OverloadedStrings #-}

module Database.Alteryx.Fields
       (
         getValue,
         getAllVariableData,
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
import qualified Data.ByteString.Char8 as BSC
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
putValue field value =
  let putFixedString :: Maybe Int -> Int -> T.Text -> (T.Text -> BS.ByteString) -> Put
      putFixedString size bytesPerCharacter text encoder = do
        let stringBS = encoder text
        let numPaddingBytes = case size of
                                Nothing -> error "putValue: No size given for string value"
                                Just x  -> bytesPerCharacter * x - BS.length stringBS
        putByteString stringBS
        replicateM_ numPaddingBytes $ putWord8 0
        putWord8 0
      size = field ^. fieldSize
      fType = field ^. fieldType
      encodeLatin1 :: T.Text -> BS.ByteString
      encodeLatin1 = BSC.pack . T.unpack
  in do
  case value of
    Just (FVBool x)          -> error "putBool unimplemented"
    Just (FVByte x)          -> do
      putWord8 $ fromIntegral x
      putWord8 0
    Just (FVInt16 x)         -> do
      putWord16le $ fromIntegral x
      putWord8 0
    Just (FVInt32 x)         -> do
      putWord32le $ fromIntegral x
      putWord8 0
    Just (FVInt64 x)         -> do
      putWord64le $ fromIntegral x
      putWord8 0
    Just (FVFixedDecimal x)  -> error "putFixedDecimal unimplemented"
    Just (FVFloat x)         -> do
      let y = realToFrac x :: CFloat
      put y
      putWord8 0
    Just (FVDouble x)        -> do
      let y = realToFrac x :: CDouble
      put y
      putWord8 0
    Just (FVString x) | fType == FTDate     -> putFixedString (Just 10) 1 x encodeUtf8
    Just (FVString x) | fType == FTTime     -> putFixedString (Just 8)  1 x encodeUtf8
    Just (FVString x) | fType == FTDateTime -> putFixedString (Just 19) 1 x encodeUtf8
    Just (FVString x)        -> putFixedString size 1 x encodeLatin1
    Just (FVWString x)       -> putFixedString size 2 x encodeUtf16LE
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
        FTByte  -> putWord8 0 >> putWord8 1
        FTInt16 -> putWord16le 0 >> putWord8 1
        FTInt32 -> putWord32le 0 >> putWord8 1
        FTInt64 -> putWord64le 0 >> putWord8 1
        FTFloat -> put (0 :: CFloat) >> putWord8 1
        FTDouble -> put (0 :: CDouble) >> putWord8 1
        x -> error $ "putValue unimplemented for null values of type " ++ show x

-- | Retrieves the bytesting representing all variable data for the current record
getAllVariableData :: Get BS.ByteString
getAllVariableData = do
  numBytes <- fromIntegral <$> getWord32le
  getByteString numBytes

-- | Retrieves the variable data portion for a single field
getVariableData :: Get (Maybe BS.ByteString)
getVariableData = do
--      numBytes <- fromIntegral <$> (`shiftR` 1) <$> getWord8
      numBytesSize <- lookAhead $ odd <$> getWord8
      numBytes <- (`shiftR` 1) <$>
                  if numBytesSize
                  then fromIntegral <$> getWord8
                  else getWord32le
      bs <- getByteString $ fromIntegral numBytes
      return $ Just bs

-- | When parsing a field that has variable data, looks ahead to grab this variable data.
linkedVariableData :: Get (Maybe BS.ByteString)
linkedVariableData = do
  offsetToVarData <- getWord32le
  let isUsingSmallStringOptimization = (offsetToVarData .&. 0x80000000) == 0 &&
                                       (offsetToVarData .&. 0x30000000) /= 0
  case offsetToVarData of
    0 -> return $ Just BS.empty
    1 -> return $ Nothing
    _ | isUsingSmallStringOptimization -> do
           let byte1 = fromIntegral $ (offsetToVarData .&. 0x000000FF)             :: Word8
           let byte2 = fromIntegral $ (offsetToVarData .&. 0x0000FF00) `shiftR` 8  :: Word8
           let byte3 = fromIntegral $ (offsetToVarData .&. 0x00FF0000) `shiftR` 16 :: Word8
           return $ Just $ BS.pack [ byte1, byte2, byte3 ]

    _ -> lookAhead $ do
           x <- getByteString $ fromIntegral $ offsetToVarData - 4
           bs <- getVariableData
           return bs



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
                       else Just $ T.takeWhile (/= '\0') $ decoder bs
            Nothing -> error "getValue: String field had no size"
        getFixedString :: Int -> (BS.ByteString -> Text) -> Get (Maybe Text)
        getFixedString = getFixedStringWithSize $ field ^. fieldSize

        getVarString :: (BS.ByteString -> Text) -> Get (Maybe Text)
        getVarString f = fmap f <$> linkedVariableData

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
         FTVString       -> fmap FVVString <$> getVarString decodeLatin1
         FTVWString      -> fmap FVVWString <$> getVarString decodeUtf16LE
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
