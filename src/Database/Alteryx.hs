module Database.Alteryx(
  numReservedSpaceBytes,
  Header(..),
  YxdbFile(..)
) where

import Control.Applicative
import Control.Monad (liftM, msum, replicateM)
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.ByteString.Lazy as BS
import Data.ByteString.Lazy.Char8 as BSC
import Data.Int
import Data.Text.Lazy
import Data.Text.Lazy.Encoding

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
      contents  :: ByteString
} deriving (Eq, Show)

data Header = Header {
      description :: ByteString, -- 64 bytes
      fileId :: Word32,
      creationDate :: Word32, -- TODO: Confirm whether this is UTC or user's local time
      flags1 :: Word32,
      flags2 :: Word32,
      metaInfoLength :: Word32,
      spatialIndexPos :: Word64,
      recordBlockIndexPos :: Word64,
      compressionVersion :: Word32,

      reservedSpace :: ByteString,
      metaInfoXml :: Text
} deriving (Eq, Show)

instance Binary YxdbFile where
    put yxdbFile = do
      put $ header yxdbFile
      -- TODO: putFixedByteString takes a 32 bit, so this limits the size of possible files needlessly
      -- TODO: toStrict forces the entire string, which is bad for performance
      putFixedByteString
        (fromIntegral $ BS.length $ contents yxdbFile) $
        contents yxdbFile

    get = do
      fHeader    <- get
      fContents  <- getRemainingLazyByteString -- TODO: This function is expensive

      return $ YxdbFile {
        header    = fHeader,
        contents  = fContents
      }

putFixedByteString :: Int -> ByteString -> Put
putFixedByteString n bs =
    let len = fromIntegral $ BS.length bs
    in if n /= len
       then error $
                "Invalid ByteString length: " ++
                (show $ len) ++
                "; Expected: " ++
                (show $ n)
       else mapM_ putWord8 $ BS.unpack bs

getFixedByteString :: Int -> Get ByteString
getFixedByteString n = BS.pack <$> replicateM n getWord8

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
        fDescription         <- getFixedByteString 64
        fFileId              <- getWord32le
        fCreationDate        <- getWord32le
        fFlags1              <- getWord32le
        fFlags2              <- getWord32le
        fMetaInfoLength      <- getWord32le
        fSpatialIndexPos     <- getWord64le
        fRecordBlockIndexPos <- getWord64le
        fCompressionVersion  <- getWord32le
        fReservedSpace       <- getFixedByteString $ fromIntegral $ numReservedSpaceBytes
        fMetaInfoXml         <- return . decodeUtf16LE =<< (getFixedByteString $ fromIntegral $ fMetaInfoLength * 2)
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

parseContents :: YxdbFile -> ByteString
parseContents yxdb = contents yxdb

-- parseYxdb :: Handle -> IO YxdbFile
-- parseYxdb handle = do
--   fCopyright <- hGetLine handle

-- type XField = Field {
--       name :: Text,
--       source :: Text,
-- }
-- type XRecordInfo = XRecordInfo [XField]

-- type DB = DB Header Body
