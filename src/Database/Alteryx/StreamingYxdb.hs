{-# LANGUAGE RankNTypes #-}

module Database.Alteryx.StreamingYxdb
       (
        blocksToRecords,
        sinkRecords,
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
import qualified Control.Monad.Trans.State.Lazy as State
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
import System.IO

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

type StatefulConduit a m b = Conduit a (State.StateT StreamingCSVStatistics m) b

recordsToBlocks :: (MonadThrow m) => RecordInfo -> StatefulConduit Record m Block
recordsToBlocks recordInfo = do
  allRecords <- CC.sinkList
  let records = Prelude.take recordsPerBlock allRecords
  if Prelude.null records
     then return ()
     else do
       let numRecords = Prelude.length records
       lift $ State.modify' (& statisticsNumRecords %~ (+numRecords))
       yield $ Block $ runPut $ M.mapM_ (putRecord recordInfo) records
       recordsToBlocks recordInfo


blocksToYxdbBytes :: (MonadThrow m) => RecordInfo -> StatefulConduit Block m BS.ByteString
blocksToYxdbBytes recordInfo = do
  -- We fill the header with padding since we don't know enough to fill it in yet
  let headerBS = BS.replicate headerPageSize 0
  yield headerBS
  let metadataBS = BSL.toStrict $ runPut (put recordInfo)
  yield metadataBS
  let putBlockWithIndex :: (MonadThrow m) => StatefulConduit Block m BS.ByteString
      putBlockWithIndex = do
        mBlock <- await
        case mBlock of
          Nothing -> return ()
          Just block -> do
            let bs = BSL.toStrict $ runPut $ put block
                bsLen = BS.length bs
            lift $ State.modify' (& statisticsBlockLengths %~ (bsLen:))
            yield bs
            putBlockWithIndex
  putBlockWithIndex

computeHeaderFromStatistics :: StreamingCSVStatistics -> Header
computeHeaderFromStatistics = undefined

computeBlockIndexFromStatistics :: StreamingCSVStatistics -> BlockIndex
computeBlockIndexFromStatistics = undefined

toBS :: Binary a => a -> BS.ByteString
toBS = BSL.toStrict . runPut . put


sinkYxdbBytes :: (MonadThrow m, MonadIO m) => Handle -> Sink BS.ByteString (State.StateT StreamingCSVStatistics m) ()
sinkYxdbBytes handle = do
  statistics <- lift State.get
  let header = computeHeaderFromStatistics statistics
      blockIndex = computeBlockIndexFromStatistics statistics
      headerBS = toBS header
      blockIndexBS = toBS blockIndex
  liftIO $ do
    hSeek handle AbsoluteSeek 0
    BS.hPut handle headerBS
    hSeek handle SeekFromEnd 0
    BS.hPut handle blockIndexBS

sinkRecords :: (MonadThrow m, MonadIO m) => Handle -> RecordInfo -> Sink Record m ()
sinkRecords handle recordInfo =
  let statefulConduit :: (MonadThrow m) => StatefulConduit Record m BS.ByteString
      statefulConduit = recordsToBlocks recordInfo =$=
                        blocksToYxdbBytes recordInfo
  in evalStateLC defaultStatistics $
     recordsToBlocks recordInfo =$=
     blocksToYxdbBytes recordInfo =$
     sinkYxdbBytes handle
