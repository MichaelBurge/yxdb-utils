module Database.Alteryx(
  Header(..),
  Content(..),
  Metadata(..),
  YxdbFile(..)
) where

import Codec.Compression.LZF.ByteString (decompressByteStringUnsafe, compressByteString)
import Control.Applicative
import Control.Monad (liftM, msum, replicateM)
import Data.Binary
import Data.Binary.Get
    (
     getByteString,
     getRemainingLazyByteString,
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
     Put(..)
    )
import Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.ByteString.Lazy.Char8 as BSC
import Data.Int
import Data.Maybe (isJust)
import Data.Text as T
import Data.Text.Encoding
import System.IO.Unsafe (unsafePerformIO)

import Foreign.Storable (sizeOf)

data DbType = WrigleyDb | WrigleyDb_NoSpatialIndex

recordsPerBlock = 0x10000
spatialIndexRecordBlockSize = 32
headerPageSize = 512

dbFileId WrigleyDb = 0x00440205
dbFileId WrigleyDb_NoSpatialIndex = 0x00440204

data YxdbFile = YxdbFile {
      header   :: Header,
      metadata :: Metadata,
      contents :: Content
} deriving (Eq, Show)

data Header = Header {
      description :: BS.ByteString, -- 64 bytes
      fileId :: Word32,
      creationDate :: Word32, -- TODO: Confirm whether this is UTC or user's local time
      flags1 :: Word32,
      flags2 :: Word32,
      metaInfoLength :: Word32,
      spatialIndexPos :: Word64,
      recordBlockIndexPos :: Word64,
      compressionVersion :: Word32,
      reservedSpace :: BS.ByteString
} deriving (Eq, Show)

newtype Metadata = Metadata Text deriving (Eq, Show)
newtype Content = Content BSL.ByteString deriving (Eq, Show)

instance Binary YxdbFile where
    put yxdbFile = do
      put $ header yxdbFile
      put $ metadata yxdbFile
      put $ contents yxdbFile

    get = do
      fHeader <- label "Header" $ isolate headerPageSize get
      fMetadata <- label "Metadata" $ isolate (fromIntegral $ 2 * (metaInfoLength fHeader)) $ get

      fContents <- label "Contents" get

      return $ YxdbFile {
        header    = fHeader,
        metadata  = fMetadata,
        contents  = fContents
      }

instance Binary Metadata where
    put (Metadata metadata) = do
      putByteString $ encodeUtf16LE metadata

    get = do
      metadataBS <- BS.concat . BSL.toChunks <$> getRemainingLazyByteString
      let metadata = decodeUtf16LE metadataBS
      return $ Metadata metadata

instance Binary Content where
    get = do
      chunks <- getContentChunks
      return $ Content $ BSL.fromChunks chunks
    put (Content content) = putContentChunks $ BSL.toChunks content

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
putContentChunks [] = flush
putContentChunks (x:xs) = do
  putContentChunk x
  putContentChunks xs

getContentChunk :: Get BS.ByteString
getContentChunk = do
  writtenSize <- getWord32le
  let compressionBitIndex = 31
  let isCompressed = not $ testBit writtenSize compressionBitIndex
  let size = clearBit writtenSize compressionBitIndex

  bs <- getByteString $ fromIntegral size
  let chunk = if isCompressed
              then decompressByteStringUnsafe bs
              else bs
  return chunk

putContentChunk :: BS.ByteString -> Put
putContentChunk bs = do
  let compressionBitIndex = 31
  let compressedChunk = compressByteString bs
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
      putWord64le $ spatialIndexPos header
      putWord64le $ recordBlockIndexPos header
      putWord32le $ compressionVersion header
      putByteString $ reservedSpace header

    get = do
        fDescription         <- label "Description" $       getByteString 64
        fFileId              <- label "FileId"              getWord32le
        fCreationDate        <- label "Creation Date"       getWord32le
        fFlags1              <- label "Flags 1"             getWord32le
        fFlags2              <- label "Flags 2"             getWord32le
        fMetaInfoLength      <- label "Metadata Length"     getWord32le
        fSpatialIndexPos     <- label "Spatial Index"       getWord64le
        fRecordBlockIndexPos <- label "Record Block"        getWord64le
        fCompressionVersion  <- label "Compression Version" getWord32le
        fReservedSpace       <- label "Reserved Space" $ (BS.concat . BSL.toChunks <$> getRemainingLazyByteString)

        return $ Header {
            description         = fDescription,
            fileId              = fFileId,
            creationDate        = fCreationDate,
            flags1              = fFlags1,
            flags2              = fFlags2,
            metaInfoLength      = fMetaInfoLength,
            spatialIndexPos     = fSpatialIndexPos,
            recordBlockIndexPos = fRecordBlockIndexPos,
            compressionVersion  = fCompressionVersion,
            reservedSpace       = fReservedSpace
        }

-- parseYxdb :: Handle -> IO YxdbFile
-- parseYxdb handle = do
--   fCopyright <- hGetLine handle

-- type XField = Field {
--       name :: Text,
--       source :: Text,
-- }
-- type XRecordInfo = XRecordInfo [XField]

-- type DB = DB Header Body
