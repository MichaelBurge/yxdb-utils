module Database.Alteryx(
  Header(..),
  YxdbFile(..)
) where

import Data.Binary
import Data.ByteString
import Data.Text
import Data.Text.Binary

data DbType = WrigleyDb | WrigleyDb_NoSpatialIndex

recordsPerBlock = 0x10000
spatialIndexRecordBlockSize = 32

dbFileId WrigleyDb = 0x00440205
dbFileId WrigleyDb_NoSpatialIndex = 0x00440204

data YxdbFile = YxdbFile {
      copyright :: Text,
      header    :: Header,
      contents  :: ByteString
} deriving (Eq, Show)

-- As little parsing as possible is done here
data Header = Header {
      description :: ByteString,
      fileId :: Word32,
      creationDate :: Word32, -- TODO: Confirm whether this is UTC or user's local time
      flags1 :: Word32,
      flags2 :: Word32,
      metaInfoLength :: Word32,
      spatialIndexPos :: Word64,
      recordBlockIndexPos :: Word64,
      compressionVersion :: Word32,
      metaInfoXml :: ByteString
} deriving (Eq, Show)

instance Binary YxdbFile where
    put yxdbFile = do
      put $ copyright yxdbFile
      put $ header yxdbFile
      put $ contents yxdbFile

    get = do
      fCopyright <- get -- TODO: Ensure we only get the first line
      fHeader    <- get
      fContents  <- get

      return $ YxdbFile {
        copyright = fCopyright,
        header    = fHeader,
        contents  = fContents
      }

instance Binary Header where
    put header = do
      put $ description header
      put $ fileId header
      put $ creationDate header
      put $ flags1 header
      put $ flags2 header
      put $ metaInfoLength header
      put $ spatialIndexPos header
      put $ recordBlockIndexPos header
      put $ compressionVersion header
      put $ metaInfoXml header

    get = do
        fDescription         <- get -- TODO: This should be a fixed amount
        fFileId              <- get
        fCreationDate        <- get
        fFlags1              <- get
        fFlags2              <- get
        fMetaInfoLength      <- get
        fSpatialIndexPos     <- get
        fRecordBlockIndexPos <- get
        fCompressionVersion  <- get
        fMetaInfoXml         <- get  -- TODO: This should be a fixed amount dependent on the metaInfoLength
        return $ Header {
            description = fDescription,
            fileId = fFileId,
            creationDate = fCreationDate,
            flags1 = fFlags1,
            flags2 = fFlags2,
            metaInfoLength = fMetaInfoLength,
            spatialIndexPos = fSpatialIndexPos,
            recordBlockIndexPos = fRecordBlockIndexPos,
            compressionVersion = fCompressionVersion,
            metaInfoXml = fMetaInfoXml
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
