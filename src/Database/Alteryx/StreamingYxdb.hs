module Database.Alteryx.StreamingYxdb
       (
         blocksToRecords,
         blocksToYxdbBytes,
         getMetadata,
         recordsToBlocks,
         sourceFileBlocks
       )where

import Conduit
import Control.Applicative
import Control.Lens hiding (from, to)
import Control.Monad as M
import Control.Monad.Primitive as M
import Control.Monad.Trans.Resource
import qualified Control.Newtype as NT
import Data.Array.Unboxed as A
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.ByteString as BS
import Data.ByteString.Lazy as BSL
import Data.Conduit
import Data.Conduit.Binary
import Data.Conduit.Combinators as CC
import Data.Conduit.Serialization.Binary
import Data.Vector as V (toList)

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

type BlockRange = (Int, Int)
type BlockRanges = [BlockRange]

blockRanges :: YxdbMetadata -> BlockRanges
blockRanges metadata =
  let blockIndices =
        Prelude.map fromIntegral $
        A.elems $
        NT.unpack $
        metadata ^. metadataBlockIndex
      blockEnd = fromIntegral $ metadata ^. metadataHeader ^. recordBlockIndexPos
      ranges = Prelude.zip blockIndices (Prelude.tail $ blockIndices ++ return blockEnd)
  in ranges

sourceBlock :: (MonadResource m) => FilePath -> BlockRange -> Source m Block
sourceBlock  filepath (from, to) = do
  let numBytes = fromIntegral $ to - from
  rawBlock <- readRange filepath (Just from) (Just numBytes)
  let block = runGet (label ("yieldBlock: " ++ show (from, numBytes)) get) $
              BSL.fromStrict rawBlock :: Block
  yield block

sourceBlocks :: (MonadResource m) => FilePath -> BlockRanges -> Source m Block
sourceBlocks filepath ranges = forM_ ranges $ sourceBlock filepath

sourceFileBlocks :: (MonadResource m) => FilePath -> YxdbMetadata -> Source m Block
sourceFileBlocks filepath = sourceBlocks filepath . blockRanges

blocksToRecords :: (MonadThrow m) => RecordInfo -> Conduit Block m Record
blocksToRecords recordInfo =
  CC.concatMap (BSL.toChunks . NT.unpack) =$=
  conduitGet (getRecord recordInfo)

recordsToBlocks :: (MonadThrow m) => RecordInfo -> Conduit Record m Block
recordsToBlocks recordInfo = do
  allRecords <- CC.sinkList
  let records = Prelude.take recordsPerBlock allRecords
  if Prelude.null records
     then return ()
     else yield $ Block $ runPut $ M.mapM_ (putRecord recordInfo) records

blocksToYxdbBytes :: (MonadThrow m) => RecordInfo -> Conduit Block m BS.ByteString
blocksToYxdbBytes recordInfo = do
  -- We fill the header with padding since we don't know enough to fill it in yet
  let headerBS = BS.replicate headerPageSize 0
  yield headerBS
  let metadataBS = BSL.toStrict $ runPut (put recordInfo)
  yield metadataBS
  CC.map (BSL.toStrict . runPut . put)
    
