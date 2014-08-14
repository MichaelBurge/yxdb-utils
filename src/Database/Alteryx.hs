module Database.Alteryx(
  numReservedSpaceBytes,
  Header(..),
  Content(..),
  YxdbFile(..)
) where

import Codec.Compression.LZF.ByteString (decompressByteStringUnsafe, compressByteString)
import Control.Applicative
import Control.Monad (liftM, msum, replicateM)
import Data.Binary
import Data.Binary.Get (getByteString, getWord32le, getWord64le, isEmpty, Get(..))
import Data.Binary.Put (putWord32le, putWord64le, flush, Put())
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

 -- Computed from many fields in the Header type
headerSize =
    64 +
    sizeOf(undefined::Word32)*6 +
    sizeOf(undefined::Word64)*2

numReservedSpaceBytes = headerPageSize - headerSize

dbFileId WrigleyDb = 0x00440205
dbFileId WrigleyDb_NoSpatialIndex = 0x00440204

data YxdbFile = YxdbFile {
      header    :: Header,
      contents  :: Content
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

      reservedSpace :: BS.ByteString,
      metaInfoXml :: Text
} deriving (Eq, Show)

newtype Content = Content BSL.ByteString deriving (Eq, Show)

instance Binary YxdbFile where
    put yxdbFile = do
      put $ header yxdbFile
      put $ contents yxdbFile

    get = do
      fHeader   <- get
      fContents <- get

      return $ YxdbFile {
        header    = fHeader,
        contents  = fContents
      }

putFixedByteString :: Int -> BS.ByteString -> Put
putFixedByteString n bs =
    let len = fromIntegral $ BS.length bs
    in if n /= len
       then error $
                "Invalid ByteString length: " ++
                (show $ len) ++
                "; Expected: " ++
                (show $ n)
       else mapM_ putWord8 $ BS.unpack bs

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
  putFixedByteString size chunkToWrite

instance Binary Header where
    put header = do
      putFixedByteString 64 $ description header
      putWord32le $ fileId header
      putWord32le $ creationDate header
      putWord32le $ flags1 header
      putWord32le $ flags2 header
      putWord32le $ metaInfoLength header
      putWord64le $ spatialIndexPos header
      putWord64le $ recordBlockIndexPos header
      putWord32le $ compressionVersion header
      putFixedByteString numReservedSpaceBytes $ reservedSpace header
      let numHeaderBytes = (2*) $ fromIntegral $ metaInfoLength header
      putFixedByteString numHeaderBytes $ encodeUtf16LE $ metaInfoXml header

    get = do
        fDescription         <- getByteString 64
        fFileId              <- getWord32le
        fCreationDate        <- getWord32le
        fFlags1              <- getWord32le
        fFlags2              <- getWord32le
        fMetaInfoLength      <- getWord32le
        fSpatialIndexPos     <- getWord64le
        fRecordBlockIndexPos <- getWord64le
        fCompressionVersion  <- getWord32le
        fReservedSpace       <- getByteString $ fromIntegral numReservedSpaceBytes
        fMetaInfoXml         <- decodeUtf16LE <$> (getByteString $ fromIntegral $ fMetaInfoLength * 2)
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
            reservedSpace       = fReservedSpace,
            metaInfoXml         = fMetaInfoXml
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
