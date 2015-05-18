{-# LANGUAGE TemplateHaskell,OverloadedStrings #-}

module Database.Alteryx.CLI.Cydb2Csv where

import Database.Alteryx

import Control.Lens
import Control.Monad.Trans.Resource
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Data.Conduit
import Data.Conduit.Binary
import qualified Data.Conduit.Combinators as CC
import Data.Monoid
import System.Console.GetOpt
import System.Environment
import System.IO

data Settings = Settings {
      _settingFilename :: String,
      _settingMetadata :: Bool,
      _settingNumBlocks  :: Maybe Int,
      _settingNumRecords :: Maybe Int
    } deriving (Eq, Show)

makeLenses ''Settings

options :: [OptDescr (Settings -> Settings)]
options =
  let set setting = \o -> (& setting .~ Just (read o))
  in [
   Option ['m'] ["dump-metadata"] (NoArg (& settingMetadata .~ True)) "Dump the file's metadata",
   Option ['b'] ["num-blocks"] (ReqArg (set settingNumBlocks) "Number of blocks") "Only output the given number of blocks",
   Option ['r'] ["num-records"] (ReqArg (set settingNumRecords) "Number of records") "Only output the given number of records, per block"

  ]

parseOptions :: [String] -> IO ([Settings -> Settings])
parseOptions args =
  case getOpt Permute options args of
    (opts, filename:[], []) -> return $ (\o -> o & settingFilename .~ filename):opts
    (_, [], [])             -> fail $ "Must provide a filename\n" ++ usageInfo header options
    (_, _, errors)          -> fail $ concat errors ++ usageInfo header options
  where
    header = "Usage: cydb2csv [OPTIONS...] filename"

processOptions :: [Settings -> Settings] -> Settings
processOptions = foldl (flip ($)) defaultSettings

getSettings :: IO Settings
getSettings = do
  argv <- getArgs
  opts <- parseOptions argv
  return $ processOptions opts

defaultSettings :: Settings
defaultSettings = Settings {
                    _settingFilename = error "defaultSettings: Filename empty",
                    _settingMetadata = False,
                    _settingNumBlocks = Nothing,
                    _settingNumRecords = Nothing
                  }

printHeader :: CalgaryHeader -> IO ()
printHeader header = do
  T.putStrLn "Header:"
  T.putStrLn $ ("  Description: " <>) $ header ^. calgaryHeaderDescription
  T.putStrLn $ ("  FileId: " <>) $ T.pack $ show $ header ^. calgaryHeaderFileId
  T.putStrLn $ ("  CreationDate: " <>) $ T.pack $ show $ header ^. calgaryHeaderCreationDate
  T.putStrLn $ ("  IndexPosition: " <>) $ T.pack $ show $ header ^. calgaryHeaderIndexPosition
  T.putStrLn $ ("  Mystery1: " <>) $ T.pack $ show $ header ^. calgaryHeaderMystery1
  T.putStrLn $ ("  NumRecords: " <>) $ T.pack $ show $ header ^. calgaryHeaderNumRecords
  T.putStrLn $ ("  Mystery2: " <>) $ T.pack $ show $ header ^. calgaryHeaderMystery2
  T.putStrLn $ ("  Mystery3: " <>) $ T.pack $ show $ header ^. calgaryHeaderMystery3
  T.putStrLn $ ("  Mystery4: " <>) $ T.pack $ show $ header ^. calgaryHeaderMystery4
  T.putStrLn $ ("  Mystery5: " <>) $ T.pack $ show $ header ^. calgaryHeaderMystery5
  T.putStrLn $ ("  Mystery6: " <>) $ T.pack $ show $ header ^. calgaryHeaderMystery6
  T.putStrLn $ ("  Number of Blocks: " <>) $ T.pack $ show $ header ^. calgaryHeaderNumBlocks

getRecordLimiter :: (MonadThrow m) => Settings -> Conduit Record m Record
getRecordLimiter settings = case settings ^. settingNumRecords of
                              Just n  -> CC.take n
                              Nothing -> CC.map id

runCydb2Csv :: Settings -> IO ()
runCydb2Csv settings = do
  let filename = settings ^. settingFilename
  calgaryFile <- readCalgaryFileNoRecords filename
  let calgaryRecordInfo = calgaryFile ^. calgaryFileRecordInfo
      unpackRecordInfo (CalgaryRecordInfo x) = x
      recordInfo = unpackRecordInfo calgaryRecordInfo

  runResourceT $
    sourceCalgaryFileRecords filename $=
    getRecordLimiter settings =$=
    record2csv recordInfo =$=
    csv2bytes $$
    sinkHandle stdout

runMetadata :: Settings -> IO ()
runMetadata settings = do
  let filename = settings ^. settingFilename
  calgaryFile <- readCalgaryFileNoRecords filename
  printHeader $ calgaryFile ^. calgaryFileHeader
  let (CalgaryRecordInfo recordInfo) = calgaryFile ^. calgaryFileRecordInfo
  printRecordInfo recordInfo

cydb2csvMain :: IO ()
cydb2csvMain = do
  settings <- getSettings
  if settings ^. settingMetadata
     then runMetadata settings
     else runCydb2Csv settings
