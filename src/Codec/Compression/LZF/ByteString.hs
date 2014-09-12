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
import Data.ByteString.Unsafe
import Foreign.C
import GHC.Ptr
import System.IO.Unsafe (unsafePerformIO)
import Foreign.Marshal.Alloc

mapChunks :: (BS.ByteString -> BS.ByteString) -> BSL.ByteString -> BSL.ByteString
mapChunks f bs = BSL.fromChunks . map f . BSL.toChunks $ bs

decompressLazyByteString :: BSL.ByteString -> BSL.ByteString
decompressLazyByteString = mapChunks decompressByteString

compressLazyByteString :: BSL.ByteString -> BSL.ByteString
compressLazyByteString = mapChunks compressByteString

_runFunctionInNewBufferSafe ::(Ptr CChar -> Int -> Ptr CChar -> Int -> IO Int) -> BS.ByteString -> Int -> IO (Maybe BS.ByteString)
_runFunctionInNewBufferSafe f bs numOutputBytes = do
  unsafeUseAsCStringLen bs $ \(input, len) ->
    if len == 0
       then return $ Just BS.empty
       else do
         output <- mallocBytes numOutputBytes
         res <- f input len output numOutputBytes
         if res == 0
           then return Nothing
           else do
             outBs <- unsafePackMallocCStringLen (output, res)
             return $ Just outBs

_runFunctionInNewBufferWithSizeGuess ::(Ptr CChar -> Int -> Ptr CChar -> Int -> IO Int) -> BS.ByteString -> IO BS.ByteString
_runFunctionInNewBufferWithSizeGuess f bs =
    if BS.null bs
    then return $ BS.empty
    else do
           let initialGuess = 2 * (BS.length bs)
           let try size = do
               when (size <= 0) $
                   fail $
                   "Codec.Compression.LZF.ByteString: Invalid size" ++ show size
               result <- _runFunctionInNewBufferSafe f bs size
               case result of
                 Nothing -> try $ size * 2
                 Just outBs -> return outBs
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

