module Tests.Database.Utils where

import Data.ByteString as BS
import GHC.IO.Handle
import System.IO
import System.Directory

captureStdout :: IO () -> IO BS.ByteString
captureStdout f = do
  tmpd <- getTemporaryDirectory
  (tmpf, tmph) <- openTempFile tmpd "haskell_stdout"
  stdout_dup <- hDuplicate stdout
  hDuplicateTo tmph stdout
  hClose tmph
  f
  hDuplicateTo stdout_dup stdout
  bs <- BS.readFile tmpf
  removeFile tmpf
  return bs
