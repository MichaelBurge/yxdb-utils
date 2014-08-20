{-# LANGUAGE OverloadedStrings,MultiParamTypeClasses #-}

module Database.Alteryx(
  BlockIndex(..),
  Header(..),
  Blocks(..),
  Metadata(..),
  YxdbFile(..),
  FieldValue(..),
  FieldType(..),
  Field(..),
  RecordInfo(..),
  headerPageSize,
  numMetadataBytesActual,
  numMetadataBytesHeader,
  numBlocksBytesActual,
  numBlocksBytesHeader,
  startOfBlocksByteIndex
) where

import Codec.Compression.LZF.ByteString (decompressByteStringFixed, compressByteStringFixed)
import qualified Control.Newtype as NT
import Control.Applicative
import Control.Monad as M (liftM, msum, replicateM, when, zipWithM_)
import Control.Monad.Trans.Resource (runResourceT)
import Data.Array.IArray (listArray, bounds, elems)
import Data.Array.Unboxed (UArray)
import Data.Bimap as Bimap (Bimap(..), fromList, lookup, lookupR)
import Data.Binary
import Data.Binary.C ()
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
import Data.Binary.C
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
import Foreign.C.Types
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
      records    :: [Record],
      blocks     :: Blocks,
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

newtype Record = Record [ FieldValue ] deriving (Eq, Show)
newtype RecordInfo = RecordInfo [ Field ] deriving (Eq, Show)
newtype Metadata = Metadata RecordInfo deriving (Eq, Show)
newtype Blocks = Blocks BSL.ByteString deriving (Eq, Show)
newtype BlockIndex = BlockIndex (UArray Int Int64) deriving (Eq, Show)

numBytes x = fromIntegral $ BSL.length $ runPut $ put x

numMetadataBytesHeader :: Header -> Int
numMetadataBytesHeader header = fromIntegral $ 2 * (metaInfoLength $ header)

numMetadataBytesActual :: Metadata -> Int
numMetadataBytesActual metadata = numBytes metadata

numBlocksBytesHeader :: Header -> Int
numBlocksBytesHeader header =
    let start = headerPageSize + (numMetadataBytesHeader header)
        end =  (fromIntegral $ recordBlockIndexPos header)
    in end - start

numBlocksBytesActual :: Blocks -> Int
numBlocksBytesActual blocks = numBytes blocks

startOfBlocksByteIndex :: Header -> Int
startOfBlocksByteIndex header =
    headerPageSize + (numMetadataBytesHeader header)

parseRecordsUntil :: RecordInfo -> Get [Record]
parseRecordsUntil recordInfo = do
  done <- isEmpty
  if done
    then return $ []
    else do
      record <- getRecord recordInfo
      records <- parseRecordsUntil recordInfo
      return $ (:) record records

instance Binary YxdbFile where
    put yxdbFile = do
      put $ header yxdbFile
      put $ metadata yxdbFile
      put $ blocks yxdbFile
      put $ blockIndex yxdbFile

    get = do
      fHeader     <- label "Header" $ isolate (fromIntegral headerPageSize) get
      fMetadata   <- label "Metadata" $ isolate (numMetadataBytesHeader fHeader) $ get

      let numBlocksBytes = numBlocksBytesHeader $ fHeader

      fBlocks    <- label ("Blocks of size " ++ show numBlocksBytes) $
                    isolate numBlocksBytes get
      fBlockIndex <- label "Block Index" get
      let recordInfo = NT.unpack fMetadata
      let fRecords = runGet (label "Records" $ parseRecordsUntil recordInfo) $ NT.unpack fBlocks

      return $ YxdbFile {
        header     = fHeader,
        metadata   = fMetadata,
        records    = fRecords,
        blocks     = fBlocks,
        blockIndex = fBlockIndex
      }

instance NT.Newtype Metadata RecordInfo where
    pack = Metadata
    unpack (Metadata x) = x

instance NT.Newtype Blocks BSL.ByteString where
    pack = Blocks
    unpack (Blocks x) = x

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
          transformMetaInfo (Metadata recordInfo) =
              Element "MetaInfo" Map.empty [ transformRecordInfo recordInfo]
          transformToDocument node = Document (Prologue [] Nothing []) node []

          renderMetaInfo metadata =
              encodeUtf16LE $
              TL.toStrict $
              flip TL.snoc '\0' $
              flip TL.snoc '\n' $
              renderText def $
              transformToDocument $
              transformMetaInfo metadata
      in putByteString $ renderMetaInfo metadata

    get = do
      bs <- BS.concat . BSL.toChunks <$>
            getRemainingLazyByteString
      when (BS.length bs < 4) $ fail $ "No trailing newline and null: " ++ show bs
      let text = T.init $ T.init $ decodeUtf16LE bs
      let document = parseText_ def $ TL.fromStrict text
      let cursor = fromDocument document
      let recordInfos = parseXmlRecordInfo cursor
      case recordInfos of
        [] -> fail "No RecordInfo entries found"
        x:[] -> return $ Metadata $ x
        otherwise -> fail "Too many RecordInfo entries found"

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

instance Binary Blocks where
    get = do
      chunks <- getBlocks
      return $ Blocks $ BSL.fromChunks chunks
    put (Blocks blocks) = putBlocks $ BSL.toChunks blocks

putRecord :: Record -> Put
putRecord (Record fieldValues) = mapM_ putValue fieldValues

getRecord :: RecordInfo -> Get Record
getRecord (RecordInfo fields) = Record <$> mapM getValue fields

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

getBlocks :: Get [BS.ByteString]
getBlocks = do
  done <- isEmpty
  if (done)
     then return $ []
     else do
       block <- getBlock
       remainingBlocks <- getBlocks
       return $ block:remainingBlocks

putBlocks :: [BS.ByteString] -> Put
putBlocks []  = putBlock BS.empty
putBlocks [x] = putBlock x
putBlocks (x:xs) = do
  putBlock x
  putBlocks xs

getBlock :: Get BS.ByteString
getBlock = do
  writtenSize <- label "Block size" getWord32le
  let compressionBitIndex = 31
  let isCompressed = not $ testBit writtenSize compressionBitIndex
  let size = fromIntegral $ clearBit writtenSize compressionBitIndex

  bs <- label ("Block of size " ++ show size) $ isolate size $ getByteString $ size
  let chunk = if isCompressed
              then case decompressByteStringFixed bufferSize bs of
                     Nothing -> fail "Unable to decompress. Increase buffer size?"
                     Just x -> return $ x
              else return bs
  chunk

putBlock :: BS.ByteString -> Put
putBlock bs = do
  let compressionBitIndex = 31
  let compressedBlock = compressByteStringFixed ((BS.length bs)-1) bs
  let blockToWrite = case compressedBlock of
                       Nothing -> bs
                       Just x  -> x
  let size = BS.length blockToWrite
  let writtenSize = if isJust compressedBlock
                    then size
                    else setBit size compressionBitIndex
  putWord32le $ fromIntegral writtenSize
  putByteString blockToWrite

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
