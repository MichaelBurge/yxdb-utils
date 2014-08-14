module Codec.Compression.LZF.ByteString (
  compressByteString,
  compressByteStringUnsafe,
  compressLazyByteString,
  compressLazyByteStringUnsafe,
  decompressByteString,
  decompressByteStringUnsafe,
  decompressLazyByteString,
  decompressLazyByteStringUnsafe
) where

import Codec.Compression.LZF (compress, decompress)
import Control.Monad (when)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Foreign.C
import Foreign.C.Error (getErrno)
import System.IO.Unsafe (unsafePerformIO)
import Foreign.Marshal.Alloc (allocaBytes)

mapChunks :: (BS.ByteString -> Maybe BS.ByteString) -> BSL.ByteString -> Maybe BSL.ByteString
mapChunks f bs = (return . BSL.fromChunks) =<< (mapM f $ BSL.toChunks bs)

mapChunksUnsafe :: (BS.ByteString -> BS.ByteString) -> BSL.ByteString -> BSL.ByteString
mapChunksUnsafe f bs = BSL.fromChunks $ map f $ BSL.toChunks bs

decompressLazyByteString :: BSL.ByteString -> Maybe BSL.ByteString
decompressLazyByteString = mapChunks decompressByteString

decompressLazyByteStringUnsafe :: BSL.ByteString -> BSL.ByteString
decompressLazyByteStringUnsafe = mapChunksUnsafe decompressByteStringUnsafe

compressLazyByteString :: BSL.ByteString -> Maybe BSL.ByteString
compressLazyByteString = mapChunks compressByteString

compressLazyByteStringUnsafe :: BSL.ByteString -> BSL.ByteString
compressLazyByteStringUnsafe = mapChunksUnsafe compressByteStringUnsafe

_runFunctionInNewBufferSafe f bs = do
  BS.useAsCStringLen bs $ \(input, len) ->
    if len == 0
       then return $ Just BS.empty
       else do
         allocaBytes len $ \output -> do
           res <- f input len output (2*len + 1)
           if res == 0
              then return Nothing
              else do
                bs <- BS.packCStringLen(output, res)
                return $ Just bs

_runFunctionInNewBufferUnsafe id f bs = do
  result <- _runFunctionInNewBufferSafe f bs
  case result of
    Nothing -> throwErrno id
    Just x -> return $ x

decompressByteString :: BS.ByteString -> Maybe BS.ByteString
decompressByteString bs =
    let f = decompress
    in unsafePerformIO $ _runFunctionInNewBufferSafe f bs

decompressByteStringUnsafe :: BS.ByteString -> BS.ByteString
decompressByteStringUnsafe bs =
    let id = "Codec.Compression.LZF.ByteString.decompressByteStringUnsafe"
        f = decompress
    in unsafePerformIO $ _runFunctionInNewBufferUnsafe id f bs

compressByteString :: BS.ByteString -> Maybe BS.ByteString
compressByteString bs =
    let f = compress
    in unsafePerformIO $ _runFunctionInNewBufferSafe f bs

compressByteStringUnsafe :: BS.ByteString -> BS.ByteString
compressByteStringUnsafe bs =
    let id = "Codec.Compression.LZF.ByteString.compressByteString"
        f = compress
    in unsafePerformIO $ _runFunctionInNewBufferUnsafe id f bs
