{-# LANGUAGE TemplateHaskell,OverloadedStrings #-}

import Database.Alteryx

import Control.Lens
import Control.Monad.Trans.Resource
import Data.Text
import Data.Conduit
import System.Console.GetOpt (getOpt)
import System.Environment
import System.IO

data Settings = Settings {
      _settingFilename :: T.Text
    } deriving (Eq, Show)

defaultSettings :: Settings
defaultSettings = Settings {
                    _settingFilename = error "defaultSettings: Filename empty"
                  }

runCydb2Csv :: Settings -> IO ()
runCydb2Csv settings = do
  runResourceT $
    sourceCalgaryFileRecords $=
    record2csv recordInfo =$=
    csv2bytes $$
    sinkHandle stdout

cydb2csvMain :: IO ()
cydb2csvMain = do
  settings <- getSettings
  runCydb2Csv settings
