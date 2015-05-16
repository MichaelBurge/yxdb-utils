{-# LANGUAGE OverloadedStrings,MultiParamTypeClasses #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Database.Alteryx.Serialization
    (
      buildBlock,
      buildRecord,
      calgaryHeaderSize,
      dbFileId,
      getCalgaryRecords,
      getCalgaryBlockIndex,
      getRecord,
      getValue,
      putRecord,
      putValue,
      headerPageSize,
      miniblockThreshold,
      numMetadataBytesActual,
      numMetadataBytesHeader,
      numBlockBytesActual,
      numBlockBytesHeader,
      parseRecordsUntil,
      recordsPerBlock,
      startOfBlocksByteIndex
    ) where

import Database.Alteryx.Fields
import Database.Alteryx.Types

import Blaze.ByteString.Builder
import Codec.Compression.LZF.ByteString (decompressByteStringFixed, compressByteStringFixed)
import qualified Control.Newtype as NT
import Control.Applicative
import Control.Lens
import Control.Monad as M
import Control.Monad.Loops
import Data.Array.IArray (listArray, bounds, elems)
import Data.Binary
import Data.Binary.C ()
import Data.Binary.Get
import Data.Binary.Put
import Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Conduit
import Data.Conduit.List (sourceList)
import Data.Conduit.Lazy (lazyConsume)
import qualified Data.Map as Map
import Data.Maybe (isJust, listToMaybe)
import Data.Monoid
import Data.ReinterpretCast (floatToWord, wordToFloat, doubleToWord, wordToDouble)
import Data.Text as T
import Data.Text.Encoding
import qualified Data.Text.Lazy as TL
import Data.Time.Clock.POSIX
import qualified Data.Vector as V
import System.IO.Unsafe (unsafePerformIO)
import Text.XML hiding (renderText)
import Text.XML.Cursor as XMLC
    (
     Cursor,
     ($.//),
     attribute,
     element,
     fromDocument
    )
import Text.XML.Stream.Render (renderText)
import Text.XML.Unresolved (toEvents)

-- | Number of records before each block is flushed and added to the block index
recordsPerBlock :: Int
recordsPerBlock = 0x10000

spatialIndexRecordBlockSize = 32

-- | Number of bytes taken by the fixed header
headerPageSize :: Int
headerPageSize = 512

-- | Number of bytes taken by the Calgary format's header
calgaryHeaderSize :: Int
calgaryHeaderSize = 8192

-- | When writing miniblocks, how many bytes should each miniblock aim for?
miniblockThreshold :: Int
miniblockThreshold = 0x10000

-- | When decompressing miniblocks, how many bytes should be allocated for the output?
bufferSize :: Int
bufferSize = 0x40000

dbFileId :: DbType -> Word32
dbFileId WrigleyDb = 0x00440205
dbFileId WrigleyDb_NoSpatialIndex = 0x00440204
dbFileId CalgaryDb = 0x00450101

-- Start of metadata: 8196
-- End of metadata: 10168
-- 1972 = 3da * 2 = 986 * 2

numBytes :: (Binary b, Num t) => b -> t
numBytes x = fromIntegral $ BSL.length $ runPut $ put x

numMetadataBytesHeader :: Header -> Int
numMetadataBytesHeader header = fromIntegral $ 2 * (header ^. metaInfoLength)

numMetadataBytesActual :: RecordInfo -> Int
numMetadataBytesActual recordInfo = numBytes recordInfo

numBlockBytesHeader :: Header -> Int
numBlockBytesHeader header =
    let start = headerPageSize + (numMetadataBytesHeader header)
        end =  (fromIntegral $ header ^. recordBlockIndexPos)
    in end - start

numBlockBytesActual :: Block -> Int
numBlockBytesActual block = numBytes block

startOfBlocksByteIndex :: Header -> Int
startOfBlocksByteIndex header =
    headerPageSize + (numMetadataBytesHeader header)

parseRecordsUntil :: RecordInfo -> Get [Record]
parseRecordsUntil recordInfo = do
  done <- isEmpty
  if done
    then return $ []
    else (:) <$> getRecord recordInfo <*> parseRecordsUntil recordInfo


-- | This binary instance is really slow because the YxdbFile type stores a list of records. Use the Conduit functions instead.
instance Binary YxdbFile where
    put yxdbFile = do
      put $ yxdbFile ^. yxdbFileHeader
      put $ yxdbFile ^. yxdbFileMetadata
      mapM_ (putRecord $ yxdbFile ^. yxdbFileMetadata) $ yxdbFile ^. yxdbFileRecords
      put $ yxdbFile ^. yxdbFileBlockIndex

    get = do
      fHeader     <- label "Header" $ isolate (fromIntegral headerPageSize) get
      fMetadata   <- label "Metadata" $ isolate (numMetadataBytesHeader fHeader) $ get

      let numBlockBytes = numBlockBytesHeader $ fHeader

      fBlocks    <- label ("Blocks of size " ++ show numBlockBytes) $
                    isolate numBlockBytes get :: Get Block
      fBlockIndex <- label "Block Index" get
      let fRecords = runGet (label "Records" $ parseRecordsUntil fMetadata) $ NT.unpack fBlocks

      return $ YxdbFile {
        _yxdbFileHeader     = fHeader,
        _yxdbFileMetadata   = fMetadata,
        _yxdbFileRecords    = fRecords,
        _yxdbFileBlockIndex = fBlockIndex
      }

instance Binary CalgaryRecordInfo where
    put calgaryRecordInfo = error "CalgaryRecordInfo: put undefined"
    get = CalgaryRecordInfo <$> getCalgaryRecordInfo

-- Start: 27bc = 10172
-- End: 2826 = 10278
-- Diff: 106 = 6A

-- Start: 27c1 = 10177
-- End: 2998 = 10648
-- Diff: 1D7 = 471

-- 8192 byte header
-- Read 4 bytes to get number of UTF-16 characters(so double for number of bytes)
-- blockSize is 16-bit
-- block is a 32767-byte compressed buffer
-- Is follow


getCalgaryRecords :: CalgaryRecordInfo -> Get (V.Vector Record)
getCalgaryRecords (CalgaryRecordInfo recordInfo) = do
  mystery1 <- getWord32le -- 0

  mystery2 <- getWord32le -- 1: Number of records?
  mystery3 <- getWord16le -- 0

  V.replicateM (fromIntegral mystery2) $ (getRecord recordInfo)
-- --  error $ show [ show mystery1, show mystery2, show mystery3 ]

--   x <- getRecord recordInfo
--   y <- getRecord recordInfo
--   error $ show y

--   byte <- getValue $ Field "byte" FTByte Nothing Nothing

--   int16 <- getValue $ Field "int16" FTInt16 Nothing Nothing

--   int32 <- getValue $ Field "int32" FTInt32 Nothing Nothing

--   int64 <- getValue $ Field "int64" FTInt64 Nothing Nothing

--   decimal <- getValue $ Field "decimal" FTFixedDecimal (Just 7) Nothing

--   mystery6 <- getWord32le -- 0

--   float <- getValue $ Field "float" FTFloat Nothing Nothing

--   double <- getValue $ Field "double" FTDouble Nothing Nothing

--   string <- getValue $ Field "string" FTString (Just 7) Nothing

--   wstring <- getValue $ Field "wstring "FTWString (Just 2) Nothing

--   let vfield = Field "vstring" FTVString Nothing Nothing
--       vwfield = Field "vwstring" FTVWString Nothing Nothing

--   vstring <- getValue vfield
--   vwstring <- getValue vwfield

--   date <- getValue $ Field "date" FTDate Nothing Nothing
--   time <- getValue $ Field "time" FTTime Nothing Nothing
--   datetime <- getValue $ Field "datetime" FTDateTime Nothing Nothing

--   mystery7 <- getWord32le -- 13

-- --  error $ show [
-- --             show mystery1, show mystery2, show mystery3, show mystery4, show mystery5,
-- --                  show mystery6, show mystery7
-- --            ]

--   error $ show [
--              show byte,
--              show int16,
--              show int32,
--              show int64,
--              show decimal,
--              show float,
--              show double,
--              show string,
--              show wstring,
--              show vstring,
--              show vwstring,
--              show date,
--              show time,
--              show datetime
--             ]




--   vfieldVarBs <- getVariableData
--   vwfieldVarBs <- getVariableData

--   remainder <- getRemainingLazyByteString
--   error $ show remainder
--   error $ show [
-- --             show mystery1,
--              show byte,
-- --             show byteNul,
-- --             show mystery2,
-- --             show short,
-- --             show shortNul,

-- --             show mystery3,
-- --             show int,
-- --             show intNul,
--              show int64,
-- --             show int64Nul
--              show remainder
--             ]
--  -- error $ show [ mystery1, byte, byteNul, mystery2, short, shortNul, remainder ]

instance Binary CalgaryFile where
    put calgaryFile = error "CalgaryFile: put undefined"
    get = do
      fHeader <- label "Header" $ isolate (fromIntegral calgaryHeaderSize) get :: Get CalgaryHeader
      fNumMetadataBytes <- (2*) <$> fromIntegral <$> getWord32le
      fRecordInfo <- label "Metadata" $ isolate fNumMetadataBytes $ get :: Get CalgaryRecordInfo
      let numRecords = fromIntegral $ fHeader ^. calgaryHeaderNumRecords
      let readOneBlock = do
            blockSize <- getWord16le
            block <- getByteString $ fromIntegral blockSize
            let decompressed = decompressByteStringFixed 100000 block

            let records = runGet (getCalgaryRecords fRecordInfo ) $
                          BSL.fromStrict $ case decompressed of Just x  -> x
            return records
          readAllBlocks remainingRecords = do
            if remainingRecords > 0
              then do
                records <- readOneBlock
                let newRecords = remainingRecords - V.length records
                (records :) <$> readAllBlocks newRecords
              else return []
      recordses <- readAllBlocks numRecords :: Get [ V.Vector Record ]

      blockIndex <- getCalgaryBlockIndex
      -- Should be done by here

      let result = CalgaryFile {
                   _calgaryFileHeader = fHeader,
                   _calgaryFileRecordInfo = fRecordInfo,
                   _calgaryFileRecords = recordses,
                   _calgaryFileIndex = blockIndex
                 }
      error $ show result

getCalgaryBlockIndex :: Get CalgaryBlockIndex
getCalgaryBlockIndex = do
      mystery1 <- getWord64le
      indices <- V.fromList <$> untilM' getWord64le isEmpty
      return $ CalgaryBlockIndex $ V.map fromIntegral indices

documentToTextWithoutXMLHeader :: Document -> T.Text
documentToTextWithoutXMLHeader document =
  let events = Prelude.tail $ toEvents $ toXMLDocument document
  in T.concat $
     unsafePerformIO $
     lazyConsume $
     sourceList events $=
     renderText def

getRecordInfoText :: Bool -> Get T.Text
getRecordInfoText isNullTerminated = do
  if isNullTerminated
     then do
       bs <- BS.concat . BSL.toChunks <$>
             getRemainingLazyByteString
       when (BS.length bs < 4) $ fail $ "No trailing newline and null: " ++ show bs
       let text = T.init $ T.init $ decodeUtf16LE bs
       return text
     else decodeUtf16LE <$> BS.concat . BSL.toChunks <$> getRemainingLazyByteString

getCalgaryRecordInfo :: Get RecordInfo
getCalgaryRecordInfo = do
  text <- getRecordInfoText False
  let document = parseText_ def $ TL.fromStrict text
      cursor = fromDocument document
      recordInfos = parseXmlRecordInfo cursor
  case recordInfos of
    []   -> fail "No RecordInfo entries found"
    x:[] -> return x
    xs   -> fail "Too many RecordInfo entries found"

getYxdbRecordInfo :: Get RecordInfo
getYxdbRecordInfo = do
  text <- getRecordInfoText True
  let document = parseText_ def $ TL.fromStrict text
      cursor = fromDocument document
      recordInfos = parseXmlRecordInfo cursor
  case recordInfos of
    []   -> fail "No RecordInfo entries found"
    x:[] -> return x
    xs   -> fail "Too many RecordInfo entries found"

instance Binary RecordInfo where
    put metadata =
      let fieldMap :: Field -> Map.Map Name Text
          fieldMap field =
              let
                  requiredAttributes =
                      [
                       ("name", field ^. fieldName),
                       ("type", renderFieldType $ field ^. fieldType)
                      ]
                  sizeAttributes =
                      case field ^. fieldSize of
                        Nothing -> [ ]
                        Just x -> [ ("size", T.pack $ show x) ]
                  scaleAttributes =
                      case field ^. fieldScale of
                        Nothing -> [ ]
                        Just x -> [ ("scale", T.pack $ show x) ]
              in Map.fromList $
                 Prelude.concat $
                 [ requiredAttributes, sizeAttributes, scaleAttributes ]
          transformField field =
              NodeElement $
              Element "Field" (fieldMap field) [ ]
          transformRecordInfo recordInfo =
              NodeElement $
              Element "RecordInfo" Map.empty $
              Prelude.map transformField recordInfo
          transformMetaInfo (RecordInfo recordInfo) =
              Element "MetaInfo" Map.empty [ transformRecordInfo recordInfo]
          transformToDocument node = Document (Prologue [] Nothing []) node []

          renderMetaInfo metadata =
              encodeUtf16LE $
              flip T.snoc '\0' $
              flip T.snoc '\n' $
              documentToTextWithoutXMLHeader $
              transformToDocument $
              transformMetaInfo metadata
      in putByteString $ renderMetaInfo metadata

    get = getYxdbRecordInfo

parseXmlField :: Cursor -> [Field]
parseXmlField cursor = do
  let fieldCursors = cursor $.// XMLC.element "Field"
  fieldCursor <- fieldCursors

  aName <- attribute "name" fieldCursor
  aType <- attribute "type" fieldCursor

  let aDesc = listToMaybe $ attribute "description" fieldCursor
  let aSize = listToMaybe $ attribute "size" fieldCursor
  let aScale = listToMaybe $ attribute "scale" fieldCursor

  return $ Field {
               _fieldName  = aName,
               _fieldType  = parseFieldType aType,
               _fieldSize  = parseInt <$> aSize,
               _fieldScale = parseInt <$> aScale
             }

parseXmlRecordInfo :: Cursor -> [RecordInfo]
parseXmlRecordInfo cursor = do
  let recordInfoCursors = cursor $.// XMLC.element "RecordInfo"
  recordInfoCursor <- recordInfoCursors
  let fields = parseXmlField recordInfoCursor
  return $ RecordInfo fields

parseInt :: Text -> Int
parseInt text = read $ T.unpack text :: Int

-- | True if any fields have associated variable data in the variable data portion of the record.
hasVariableData :: RecordInfo -> Bool
hasVariableData (RecordInfo recordInfo) =
  let fieldHasVariableData field =
          case field ^. fieldType of
            FTVString  -> True
            FTVWString -> True
            FTBlob     -> True
            _          -> False
  in Prelude.any fieldHasVariableData recordInfo

-- | Writes a record using the provided metadata.
putRecord :: RecordInfo -> Record -> Put
putRecord recordInfo record = putByteString $ toByteString $ buildRecord recordInfo record

buildRecord :: RecordInfo -> Record -> Builder
buildRecord recordInfo@(RecordInfo fields) (Record fieldValues) =
    if hasVariableData recordInfo
    then error "putRecord: Variable data unimplemented"
    else mconcat $ Prelude.zipWith buildValue fields fieldValues

-- | Records consists of a fixed amount of data for each field, and also a possibly large amoutn of variable data at the end.
getRecord :: RecordInfo -> Get Record
getRecord recordInfo@(RecordInfo fields) = do
  record <- Record <$> mapM getValue fields
  when (hasVariableData recordInfo) $ do
    _ <- getAllVariableData
    return ()
  return record

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

instance Binary Block where
  get =
    let tryGetOne = do
          done <- isEmpty
          if done
             then return Nothing
             else Just <$> get :: Get (Maybe Miniblock)
    in NT.pack <$>
       BSL.fromChunks <$>
       Prelude.map NT.unpack <$>
       unfoldM tryGetOne
  put block = putByteString $ toByteString $ buildBlock block

buildBlock :: Block -> Builder
buildBlock (Block bs) =
    case BSL.toChunks bs of
      [] -> buildMiniblock $ Miniblock $ BS.empty
      xs -> mconcat $ Prelude.map (buildMiniblock . Miniblock) xs

instance Binary Miniblock where
  get = do
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
    Miniblock <$> chunk
  put miniblock = putByteString $ toByteString $ buildMiniblock miniblock


buildMiniblock :: Miniblock -> Builder
buildMiniblock (Miniblock bs) =
    let compressionBitIndex = 31
        compressedBlock = compressByteStringFixed ((BS.length bs)-1) bs
        blockToWrite = case compressedBlock of
          Nothing -> bs
          Just x  -> x
        size = BS.length blockToWrite
        writtenSize = if isJust compressedBlock
                      then size
                      else setBit size compressionBitIndex
    in mconcat [
            fromWord32le $ fromIntegral writtenSize,
            fromByteString blockToWrite
           ]

instance Binary Header where
    put header = do
      let actualDescriptionBS  = BS.take 64 $ encodeUtf8 $ header ^. description
      let numPaddingBytes      = fromIntegral $ 64 - BS.length actualDescriptionBS
      let paddingDescriptionBS = BSL.toStrict $ BSL.take numPaddingBytes $ BSL.repeat 0

      putByteString actualDescriptionBS
      putByteString paddingDescriptionBS
      putWord32le   $ header ^. fileId
      putWord32le   $ truncate $ utcTimeToPOSIXSeconds $ header ^. creationDate
      putWord32le   $ header ^. flags1
      putWord32le   $ header ^. flags2
      putWord32le   $ header ^. metaInfoLength
      putWord32le   $ header ^. mystery
      putWord64le   $ header ^. spatialIndexPos
      putWord64le   $ header ^. recordBlockIndexPos
      putWord64le   $ header ^. numRecords
      putWord32le   $ header ^. compressionVersion
      putByteString $ header ^. reservedSpace

    get = do
        fDescription         <- label "Description" $ decodeUtf8 <$> getByteString 64
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
        fReservedSpace       <- label "Reserved Space" $ (BSL.toStrict <$> getRemainingLazyByteString)

        return $ Header {
            _description         = fDescription,
            _fileId              = fFileId,
            _creationDate        = posixSecondsToUTCTime $ fromIntegral fCreationDate,
            _flags1              = fFlags1,
            _flags2              = fFlags2,
            _metaInfoLength      = fMetaInfoLength,
            _mystery             = fMystery,
            _spatialIndexPos     = fSpatialIndexPos,
            _recordBlockIndexPos = fRecordBlockIndexPos,
            _numRecords          = fNumRecords,
            _compressionVersion  = fCompressionVersion,
            _reservedSpace       = fReservedSpace
        }

instance Binary CalgaryHeader where
    put header = error "CalgaryHeader::put is unimplemented"
    get = do
      description   <- decodeUtf8 <$> getByteString 64
      fileId        <- getWord32le
      creationDate  <- posixSecondsToUTCTime <$> fromIntegral <$> getWord32le
      indexPosition <- getWord32le
      mystery1      <- getWord32le
      numRecords    <- getWord32le
      mystery2      <- getWord32le
      mystery3      <- getWord32le
      mystery4      <- getWord32le
      mystery5      <- getWord32le
      reserved      <- BSL.toStrict <$> getRemainingLazyByteString
      return CalgaryHeader {
                   _calgaryHeaderDescription = description,
                   _calgaryHeaderFileId = fileId,
                   _calgaryHeaderCreationDate = creationDate,
                   _calgaryHeaderIndexPosition = indexPosition,
                   _calgaryHeaderMystery1 = mystery1,
                   _calgaryHeaderNumRecords = numRecords,
                   _calgaryHeaderMystery2 = mystery2,
                   _calgaryHeaderMystery3 = mystery3,
                   _calgaryHeaderMystery4 = mystery4,
                   _calgaryHeaderMystery5 = mystery5,
                   _calgaryHeaderReserved = reserved
                 }
