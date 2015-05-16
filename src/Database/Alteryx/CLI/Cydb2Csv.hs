{-# LANGUAGE TemplateHaskell,OverloadedStrings #-}

module Database.Alteryx.CLI.Cydb2Csv where

import Database.Alteryx

import Control.Lens
import Control.Monad.Trans.Resource
import qualified Data.Text as T
import Data.Conduit
import Data.Conduit.Binary
import System.Console.GetOpt
import System.Environment
import System.IO

data Settings = Settings {
      _settingFilename :: String
    } deriving (Eq, Show)

makeLenses ''Settings

options :: [OptDescr (Settings -> Settings)]
options =
  let set setting = \o -> (& setting .~ Just (read o))
  in [
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
                    _settingFilename = error "defaultSettings: Filename empty"
                  }

runCydb2Csv :: Settings -> IO ()
runCydb2Csv settings = do
  let filename = settings ^. settingFilename
  calgaryFile <- readCalgaryFileNoRecords filename
  let calgaryRecordInfo = calgaryFile ^. calgaryFileRecordInfo
      unpackRecordInfo (CalgaryRecordInfo x) = x
      recordInfo = unpackRecordInfo calgaryRecordInfo
  runResourceT $
    sourceCalgaryFileRecords filename $=
    record2csv recordInfo =$=
    csv2bytes $$
    sinkHandle stdout

cydb2csvMain :: IO ()
cydb2csvMain = do
  settings <- getSettings
  runCydb2Csv settings
