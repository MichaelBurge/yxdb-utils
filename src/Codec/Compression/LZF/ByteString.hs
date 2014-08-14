module Codec.Compression.LZF.ByteString (
  compressByteString,
  compressLazyByteString,
  decompressByteString,
  decompressLazyByteString
) where

import Codec.Compression.LZF (compress, decompress)
import Control.Monad (when)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Foreign.C
import Foreign.Marshal.Alloc (allocaBytes)

mapChunks :: (BS.ByteString -> IO BS.ByteString) -> BSL.ByteString -> IO BSL.ByteString
mapChunks f bs = (return . BSL.fromChunks) =<< (mapM f $ BSL.toChunks bs)

decompressLazyByteString :: BSL.ByteString -> IO BSL.ByteString
decompressLazyByteString = mapChunks decompressByteString

compressLazyByteString :: BSL.ByteString -> IO BSL.ByteString
compressLazyByteString = mapChunks compressByteString

_runFunctionInNewBuffer id f bs = do
  BS.useAsCStringLen bs $ \(input, len) ->
    if len == 0
       then return BS.empty
       else do
         allocaBytes len $ \output -> do
           res <- f input len output (2*len + 1)
           when (res == 0) $ fail (id ++ ": Failed")
           BS.packCStringLen(output, res)

decompressByteString :: BS.ByteString -> IO BS.ByteString
decompressByteString bs =
    let id = "Codec.Compression.LZF.ByteString.decompressByteString"
        f = decompress
    in _runFunctionInNewBuffer id f bs

compressByteString :: BS.ByteString -> IO BS.ByteString
compressByteString bs =
    let id = "Codec.Compression.LZF.ByteString.compressByteString"
        f = compress
    in _runFunctionInNewBuffer id f bs
