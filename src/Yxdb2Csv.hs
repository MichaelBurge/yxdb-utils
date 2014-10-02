{-# LANGUAGE TemplateHaskell,OverloadedStrings #-}

import Database.Alteryx

import Control.Applicative
import Control.Lens hiding (set, setting)
import Control.Monad
import Control.Monad.State
import Control.Monad.Trans.Resource
import qualified Control.Newtype as NT
import Data.Array.Unboxed as A
import qualified Data.ByteString as BS
import Data.Conduit
import Data.Conduit.Binary
import qualified Data.Conduit.Combinators as CC
import Data.Int
import Data.Monoid
import Data.Text as T hiding (concat, foldl)
import Data.Text.IO
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Time.Format
import Prelude hiding (putStrLn)
import System.Console.GetOpt
import System.Environment
import System.IO hiding (putStrLn)
import System.Locale

data Settings = Settings {
  _settingDecompress :: Bool,
  _settingMetadata   :: Bool,
  _settingNumBlocks  :: Maybe Int,
  _settingNumRecords :: Maybe Int,
  _settingVerbose    :: Bool,
  _settingFilename   :: String
  }

makeLenses ''Settings

options :: [OptDescr (Settings -> Settings)]
options =
  let set setting = \o -> (& setting .~ Just (read o))
  in [
    Option ['b'] ["num-blocks"] (ReqArg (set settingNumBlocks) "Number of blocks") "Only output the given number of blocks",
    Option ['r'] ["num-records"] (ReqArg (set settingNumRecords) "Number of records") "Only output the given number of records, per block",
    Option ['m'] ["dump-metadata"] (NoArg (& settingMetadata .~ True)) "Dump the file's metadata",
    Option ['v'] ["verbose"] (NoArg (& settingVerbose .~ True)) "Print extra debugging information on stderr",
    Option ['d'] ["decompress-blocks"] (NoArg (& settingDecompress .~ True)) "Debug: Decompress blocks, but don't try to interpret them."
  ]

defaultSettings :: Settings
defaultSettings = Settings {
  _settingDecompress = False,
  _settingMetadata   = False,
  _settingNumBlocks  = Nothing,
  _settingNumRecords = Nothing,
  _settingVerbose    = False,
  _settingFilename   = error "defaultSettings: Filename empty"
  }

parseOptions :: [String] -> IO ([Settings -> Settings])
parseOptions args =
  case getOpt Permute options args of
    (opts, filename:[], []) -> return $ (\o -> o & settingFilename .~ filename):opts
    (_, [], [])             -> fail $ "Must provide a filename\n" ++ usageInfo header options
    (_, _, errors)          -> fail $ concat errors ++ usageInfo header options
  where
    header = "Usage: yxdb2csv [OPTIONS...] filename"

processOptions :: [Settings -> Settings] -> Settings
processOptions = foldl (flip ($)) defaultSettings

getSettings :: IO Settings
getSettings = do
  argv <- getArgs
  opts <- parseOptions argv
  return $ processOptions opts

printHeader :: YxdbMetadata -> StateT Settings IO ()
printHeader metadata = do
  settings <- get
  let header = metadata ^. metadataHeader
  liftIO $ do
    putStrLn "Header:"
    putStrLn $ ("  Description: " <>)            $ header ^. description
    putStrLn $ ("  FileId: " <>)                 $ T.pack $ show $ header ^. fileId
    putStrLn $ ("  CreationDate: " <>)           $ T.pack $ show $ header ^. creationDate
    putStrLn $ ("  Flags1: " <>)                 $ T.pack $ show $ header ^. flags1
    putStrLn $ ("  Flags2: " <>)                 $ T.pack $ show $ header ^. flags2
    putStrLn $ ("  MetaInfoLength: " <>)         $ T.pack $ show $ header ^. metaInfoLength
    putStrLn $ ("  Mystery: " <>)                $ T.pack $ show $ header ^. mystery
    putStrLn $ ("  Spatial Index Position: " <>) $ T.pack $ show $ header ^. spatialIndexPos
    putStrLn $ ("  Block Index Position: " <>)   $ T.pack $ show $ header ^. recordBlockIndexPos
    putStrLn $ ("  Number of Records: " <>)      $ T.pack $ show $ header ^. numRecords
    putStrLn $ ("  Compression Version: " <>)    $ T.pack $ show $ header ^. compressionVersion
    when (settings ^. settingVerbose) $
      putStrLn $ ("  Reserved Space: " <>)         $ T.pack $ show $ header ^. reservedSpace


printBlocks :: YxdbMetadata -> StateT Settings IO ()
printBlocks metadata =
  let printBlock :: Int64 -> StateT Settings IO ()
      printBlock x = liftIO $ putStrLn $ T.pack $ show x
  in do
  liftIO $ putStrLn "Blocks:"
  Prelude.mapM_ printBlock $ A.elems $ NT.unpack $ metadata ^. metadataBlockIndex



runMetadata :: StateT Settings IO ()
runMetadata = do
  settings <- get
  yxdbMetadata <- liftIO $ getMetadata $ settings ^. settingFilename
  printHeader yxdbMetadata
  liftIO $ printRecordInfo $ yxdbMetadata ^. metadataRecordInfo
  when (settings ^. settingVerbose) $
    printBlocks yxdbMetadata

getBlockLimiter :: (MonadThrow m) => StateT Settings IO (Conduit Block m Block)
getBlockLimiter = do
   settings <- get
   return $ case settings ^. settingNumBlocks of
              Just n  -> CC.take n
              Nothing -> CC.map id

getRecordLimiter :: (MonadThrow m) => StateT Settings IO (Conduit Record m Record)
getRecordLimiter = do
   settings <- get
   return $ case settings ^. settingNumRecords of
              Just n  -> CC.take n
              Nothing -> CC.map id

runDecompress :: StateT Settings IO ()
runDecompress = do
  settings <- get
  let filename = settings ^. settingFilename
  metadata <- liftIO $ getMetadata filename
  let recordInfo = metadata ^. metadataRecordInfo
  blockLimiter <- getBlockLimiter
  runResourceT $
    sourceFileBlocks filename metadata $=
    blockLimiter =$=
    blocksToDecompressedBytes $$
    sinkHandle stdout

runYxdb2Csv :: StateT Settings IO ()
runYxdb2Csv = do
  settings <- get
  let filename = settings ^. settingFilename
  metadata <- liftIO $ getMetadata filename
  let recordInfo = metadata ^. metadataRecordInfo
  blockLimiter <- getBlockLimiter
  recordLimiter <- getRecordLimiter

  runResourceT $
    sourceFileBlocks filename metadata $=
    blockLimiter =$=
    blocksToRecords recordInfo =$=
    recordLimiter =$=
    record2csv =$=
    csv2bytes $$
    sinkHandle stdout

main :: IO ()
main = do
  settings <- getSettings
  flip evalStateT settings $
    case () of
      _ | settings ^. settingMetadata   -> runMetadata
        | settings ^. settingDecompress -> runDecompress
        | otherwise                     -> runYxdb2Csv
