{-# LANGUAGE OverloadedStrings #-}

module Database.Alteryx(
  BlockIndex(..),
  Header(..),
  Content(..),
  Metadata(..),
  YxdbFile(..),
  FieldValue(..),
  FieldType(..),
  Field(..),
  RecordInfo(..),
  headerPageSize,
  numMetadataBytesActual,
  numMetadataBytesHeader,
  numContentBytesActual,
  numContentBytesHeader,
  startOfContentByteIndex
) where

import Codec.Compression.LZF.ByteString (decompressByteStringFixed, compressByteStringFixed)
import Control.Applicative
import Control.Monad (liftM, msum, replicateM)
import Control.Monad.Trans.Resource (runResourceT)
import Data.Array.IArray (listArray, bounds, elems)
import Data.Array.Unboxed (UArray)
import Data.Bimap as Bimap (Bimap(..), fromList, lookup, lookupR)
import Data.Binary
import Data.Binary.Get
    (
     bytesRead,
     getByteString,
     getRemainingLazyByteString,
     getWord16le,
     getWord32le,
     getWord64le,
     isEmpty,
     isolate,
     Get(..),
     label,
     remaining,
     runGet
    )
import Data.Binary.Put
    (
     putByteString,
     putWord32le,
     putWord64le,
     flush,
     Put(..),
     runPut
    )
import Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.ByteString.Lazy.Char8 as BSC
import Data.Conduit (($$), ($=), Sink(..), yield)
import Data.Decimal (Decimal(..))
import Data.Int
import qualified Data.Map as Map
import Data.Maybe (isJust, listToMaybe)
import Data.Text as T
import Data.Text.Encoding (decodeUtf16LE, encodeUtf16LE)
import qualified Data.Text.Lazy as TL
import Data.Time.Calendar (Day(..))
import Data.Time.Clock (UniversalTime(..), UTCTime(..), DiffTime(..))
import System.IO.Unsafe (unsafePerformIO)
import Text.XML
    (
     Document(..),
     Element(..),
     Node(..),
     Prologue(..),
     parseText_,
     renderText,
    )
import Text.XML.Cursor
    (
     Cursor(..),
     ($//),
     (&/),
     attribute,
     element,
     fromDocument
    )
import Text.XML.Stream.Parse
    (
     def,
     force,
     optionalAttr,
     parseBytes,
     requireAttr,
     tagName,
     tagNoAttr
    )
import Foreign.Storable (sizeOf)

data DbType = WrigleyDb | WrigleyDb_NoSpatialIndex

recordsPerBlock = 0x10000
spatialIndexRecordBlockSize = 32
headerPageSize = 512
bufferSize = 0x40000

dbFileId WrigleyDb = 0x00440205
dbFileId WrigleyDb_NoSpatialIndex = 0x00440204

data YxdbFile = YxdbFile {
      header     :: Header,
      metadata   :: Metadata,
      content   :: Content,
      blockIndex :: BlockIndex
} deriving (Eq, Show)

data Header = Header {
      description :: BS.ByteString, -- 64 bytes
      fileId :: Word32,
      creationDate :: Word32, -- TODO: Confirm whether this is UTC or user's local time
      flags1 :: Word32,
      flags2 :: Word32,
      metaInfoLength :: Word32,
      mystery :: Word32,
      spatialIndexPos :: Word64,
      recordBlockIndexPos :: Word64,
      numRecords :: Word64,
      compressionVersion :: Word32,
      reservedSpace :: BS.ByteString
} deriving (Eq, Show)

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
                | FVBlob ByteString
                | FVSpatialObject ByteString
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

newtype RecordInfo = RecordInfo [ Field ] deriving (Eq, Show)
newtype Metadata = Metadata [ RecordInfo ] deriving (Eq, Show)
newtype Content = Content BSL.ByteString deriving (Eq, Show)
newtype BlockIndex = BlockIndex (UArray Int Int64) deriving (Eq, Show)

numBytes x = fromIntegral $ BSL.length $ runPut $ put x

numMetadataBytesHeader :: Header -> Int
numMetadataBytesHeader header = fromIntegral $ 2 * (metaInfoLength $ header)

numMetadataBytesActual :: Metadata -> Int
numMetadataBytesActual metadata = numBytes metadata

numContentBytesHeader :: Header -> Int
numContentBytesHeader header =
    let start = headerPageSize + (numMetadataBytesHeader header)
        end =  (fromIntegral $ recordBlockIndexPos header)
    in end - start

numContentBytesActual :: Content -> Int
numContentBytesActual content = numBytes content

startOfContentByteIndex :: Header -> Int
startOfContentByteIndex header =
    headerPageSize + (numMetadataBytesHeader header)

instance Binary YxdbFile where
    put yxdbFile = do
      put $ header yxdbFile
      put $ metadata yxdbFile
      put $ content yxdbFile
      put $ blockIndex yxdbFile

    get = do
      fHeader     <- label "Header" $ isolate (fromIntegral headerPageSize) get
      fMetadata   <- label "Metadata" $ isolate (numMetadataBytesHeader fHeader) $ get

      metadataEnd <- fromIntegral <$> bytesRead
      let numContentBytes = (fromIntegral $ recordBlockIndexPos fHeader) - metadataEnd

      fContent    <- label "Content" $ isolate numContentBytes get
      fBlockIndex <- label "Block Index" get

      return $ YxdbFile {
        header     = fHeader,
        metadata   = fMetadata,
        content    = fContent,
        blockIndex = fBlockIndex
      }

instance Binary Metadata where
    put metadata =
      let fieldMap field =
              let
                  requiredAttributes =
                      [
                       ("name", fieldName field),
                       ("type", renderFieldType $ fieldType field)
                      ]
                  sizeAttributes =
                      case fieldSize field of
                        Nothing -> [ ]
                        Just x -> [ ("size", T.pack $ show x) ]
                  scaleAttributes =
                      case fieldScale field of
                        Nothing -> [ ]
                        Just x -> [ ("scale", T.pack $ show x) ]
              in Map.fromList $
                 Prelude.concat $
                 [ requiredAttributes, sizeAttributes, scaleAttributes ]
          transformField field =
              NodeElement $
              Element "Field" (fieldMap field) [ ]
          transformRecordInfo (RecordInfo fields ) =
              NodeElement $
              Element "RecordInfo" Map.empty $
              Prelude.map transformField fields
          transformMetaInfo (Metadata recordInfos) =
              Element "MetaInfo" Map.empty $
              Prelude.map transformRecordInfo recordInfos
          transformToDocument node = Document (Prologue [] Nothing []) node []

          renderMetaInfo metadata =
              encodeUtf16LE $
              TL.toStrict $
              renderText def $
              transformToDocument $
              transformMetaInfo metadata
      in putByteString $ renderMetaInfo metadata

    get = do
      text <- decodeUtf16LE <$> BS.concat . BSL.toChunks <$> getRemainingLazyByteString
      let document = parseText_ def $ TL.fromStrict text
      let cursor = fromDocument document
      let recordInfos = parseXmlRecordInfo cursor
      return $ Metadata recordInfos

parseXmlField :: Cursor -> [Field]
parseXmlField cursor = do
  let fieldCursors = cursor $// element "Field"
  fieldCursor <- fieldCursors

  aName <- attribute "name" fieldCursor
  aType <- attribute "type" fieldCursor

  let aDesc = listToMaybe $ attribute "description" fieldCursor
  let aSize = listToMaybe $ attribute "size" fieldCursor
  let aScale = listToMaybe $ attribute "scale" fieldCursor

  return $ Field {
               fieldName  = aName,
               fieldType  = parseFieldType aType,
               fieldSize  = parseInt <$> aSize,
               fieldScale = parseInt <$> aScale
             }

parseXmlRecordInfo :: Cursor -> [RecordInfo]
parseXmlRecordInfo cursor = do
  let recordInfoCursors = cursor $// element "RecordInfo"
  recordInfoCursor <- recordInfoCursors
  let fields = parseXmlField recordInfoCursor
  return $ RecordInfo fields

parseInt :: Text -> Int
parseInt text = read $ T.unpack text :: Int

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

instance Binary Content where
    get = do
      chunks <- getContentChunks
      return $ Content $ BSL.fromChunks chunks
    put (Content content) = putContentChunks $ BSL.toChunks content

instance Binary BlockIndex where
    get = do
      arraySize <- label "Index Array Size" $ fromIntegral <$> getWord32le
      let numBlockIndexBytes = arraySize * 8
      blocks <- label ("Reading block of size " ++ show arraySize) $
                isolate numBlockIndexBytes $
                replicateM arraySize (fromIntegral <$> getWord64le)
      return $ BlockIndex $ listArray (0, arraySize-1) blocks

    put (BlockIndex blockIndex) = do
      let (_, iMax) = bounds blockIndex
      putWord32le $ fromIntegral $ iMax + 1
      mapM_ (putWord64le . fromIntegral) $ elems blockIndex

getContentChunks :: Get [BS.ByteString]
getContentChunks = do
  done <- isEmpty
  if (done)
     then return $ []
     else do
       chunk <- getContentChunk
       remainingChunks <- getContentChunks
       return $ chunk:remainingChunks

putContentChunks :: [BS.ByteString] -> Put
putContentChunks [] = putContentChunk BS.empty
putContentChunks [x] = putContentChunk x
putContentChunks (x:xs) = do
  putContentChunk x
  putContentChunks xs

getContentChunk :: Get BS.ByteString
getContentChunk = do
  writtenSize <- label "Content chunk size" getWord32le
  let compressionBitIndex = 31
  let isCompressed = not $ testBit writtenSize compressionBitIndex
  let size = fromIntegral $ clearBit writtenSize compressionBitIndex

  bs <- label ("Content chunk of size " ++ show size) $ isolate size $ getByteString $ size
  let chunk = if isCompressed
              then case decompressByteStringFixed bufferSize bs of
                     Nothing -> fail "Unable to decompress. Increase buffer size?"
                     Just x -> return $ x
              else return bs
  chunk

putContentChunk :: BS.ByteString -> Put
putContentChunk bs = do
  let compressionBitIndex = 31
  let compressedChunk = compressByteStringFixed ((BS.length bs)-1) bs
  let chunkToWrite = case compressedChunk of
                       Nothing -> bs
                       Just x  -> x
  let size = BS.length chunkToWrite
  let writtenSize = if isJust compressedChunk
                    then size
                    else setBit size compressionBitIndex
  putWord32le $ fromIntegral writtenSize
  putByteString chunkToWrite

instance Binary Header where
    put header = do
      putByteString $ description header
      putWord32le $ fileId header
      putWord32le $ creationDate header
      putWord32le $ flags1 header
      putWord32le $ flags2 header
      putWord32le $ metaInfoLength header
      putWord32le $ mystery header
      putWord64le $ spatialIndexPos header
      putWord64le $ recordBlockIndexPos header
      putWord64le $ numRecords header
      putWord32le $ compressionVersion header
      putByteString $ reservedSpace header

    get = do
        fDescription         <- label "Description" $       getByteString 64
        fFileId              <- label "FileId"              getWord32le
        fCreationDate        <- label "Creation Date"       getWord32le
        fFlags1              <- label "Flags 1"             getWord32le
        fFlags2              <- label "Flags 2"             getWord32le
        fMetaInfoLength      <- label "Metadata Length"     getWord32le
        fMystery             <- label "Mystery Field"       getWord32le
        fSpatialIndexPos     <- label "Spatial Index"       getWord64le
        fRecordBlockIndexPos <- label "Record Block"        getWord64le
        fNumRecords          <- label "Num Records"         getWord64le
        fCompressionVersion  <- label "Compression Version" getWord32le
        fReservedSpace       <- label "Reserved Space" $ (BS.concat . BSL.toChunks <$> getRemainingLazyByteString)

        return $ Header {
            description         = fDescription,
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
            reservedSpace       = fReservedSpace
        }
