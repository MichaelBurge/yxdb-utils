module Database.Alteryx.StreamingYxdb where

import Control.Applicative
import Control.Lens hiding (from, to)
import Control.Monad
import Control.Monad.Trans.Resource
import qualified Control.Newtype as NT
import Data.Array.Unboxed as A
import Data.Binary
import Data.Binary.Get
import Data.ByteString as BS
import Data.ByteString.Lazy as BSL
import Data.Conduit
import Data.Conduit.Binary
import Data.Conduit.Combinators as CC

import Database.Alteryx.Serialization
import Database.Alteryx.Types

readRange :: (MonadResource m) => FilePath -> Maybe Int -> Maybe Int -> m BS.ByteString
readRange filepath from to = sourceFileRange filepath (fromIntegral <$> from) (fromIntegral <$> to) $$ fold

getMetadata :: FilePath -> IO YxdbMetadata
getMetadata filepath = runResourceT $ do
  headerBS <- readRange filepath Nothing (Just headerPageSize)
  let header = decode $ BSL.fromStrict headerBS :: Header

  recordInfoBS <- readRange filepath (Just headerPageSize) (Just $ numMetadataBytesHeader header)
  let recordInfo = decode $ BSL.fromStrict recordInfoBS :: RecordInfo
  
  blockIndexBS <- readRange filepath (Just $ fromIntegral $ header ^. recordBlockIndexPos) Nothing
  let blockIndex = decode $ BSL.fromStrict blockIndexBS :: BlockIndex
  
  return YxdbMetadata {
    _metadataHeader     = header,
    _metadataRecordInfo = recordInfo,
    _metadataBlockIndex = blockIndex
    }

streamBlocks :: (MonadResource m) => FilePath -> YxdbMetadata -> Source m BS.ByteString
streamBlocks filepath metadata =
  let blockIndices = Prelude.map fromIntegral $
                     A.elems $
                     NT.unpack $
                     metadata ^. metadataBlockIndex
      ranges = Prelude.zip (Prelude.map Just blockIndices) (Prelude.map Just blockIndices ++ [Nothing])
  in forM_ ranges $ \(from, to) -> do
    rawBlock <- readRange filepath from to
    let block = runGet getBlock $ BSL.fromStrict rawBlock
    yield block
    return ()

streamRecords :: (MonadThrow m) => YxdbMetadata -> Conduit BS.ByteString m Record
streamRecords metadata = do
  mBS <- await
  case mBS of
    Nothing -> return ()
    Just bs -> yieldRecords bs
  where
    recordInfo = metadata ^. metadataRecordInfo
    yieldRecords bs = yieldMany $ runGet (parseRecordsUntil recordInfo) $ BSL.fromStrict bs
