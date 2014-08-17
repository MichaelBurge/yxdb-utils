module Codec.Compression.LZF.ByteString (
  compressByteString,
  compressByteStringFixed,
  compressLazyByteString,
  decompressByteString,
  decompressByteStringFixed,
  decompressLazyByteString,
) where

import Codec.Compression.LZF (compress, decompress)
import Control.Monad (when)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Foreign.C
import Foreign.C.Error (getErrno)
import System.IO.Unsafe (unsafePerformIO)
import Foreign.Marshal.Alloc (allocaBytes)

mapChunks :: (BS.ByteString -> BS.ByteString) -> BSL.ByteString -> BSL.ByteString
mapChunks f bs = BSL.fromChunks . map f . BSL.toChunks $ bs

decompressLazyByteString :: BSL.ByteString -> BSL.ByteString
decompressLazyByteString = mapChunks decompressByteString

compressLazyByteString :: BSL.ByteString -> BSL.ByteString
compressLazyByteString = mapChunks compressByteString

-- TODO: Calling allocaBytes every time might be inefficient
_runFunctionInNewBufferSafe f bs numOutputBytes = do
  BS.useAsCStringLen bs $ \(input, len) ->
    if len == 0
       then return $ Just BS.empty
       else do
         allocaBytes numOutputBytes $ \output -> do
           res <- f input len output numOutputBytes
           if res == 0
              then return Nothing
              else do
                bs <- BS.packCStringLen(output, res)
                return $ Just bs

_runFunctionInNewBufferWithSizeGuess f bs = do
  let initialGuess = BS.length bs
  let try size = do
        result <- _runFunctionInNewBufferSafe f bs size
        case result of
          Nothing -> try $ size * 2
          Just bs -> return $ bs
  try initialGuess

decompressByteString :: BS.ByteString -> BS.ByteString
decompressByteString bs =
    unsafePerformIO $ _runFunctionInNewBufferWithSizeGuess decompress bs 

decompressByteStringFixed :: Int -> BS.ByteString -> Maybe BS.ByteString
decompressByteStringFixed size bs =
    unsafePerformIO $ _runFunctionInNewBufferSafe decompress bs size

compressByteString :: BS.ByteString -> BS.ByteString
compressByteString bs =
    unsafePerformIO $ _runFunctionInNewBufferWithSizeGuess compress bs

compressByteStringFixed :: Int -> BS.ByteString -> Maybe BS.ByteString
compressByteStringFixed size bs =
    unsafePerformIO $ _runFunctionInNewBufferSafe compress bs size

