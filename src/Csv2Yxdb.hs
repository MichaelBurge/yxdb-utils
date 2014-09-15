{-# LANGUAGE TemplateHaskell,OverloadedStrings #-}

import Database.Alteryx

import Control.Lens
import Control.Monad.State
import System.Console.GetOpt
import System.Environment

data Settings = Settings {
  _settingFilename :: String,
  _settingMetadata :: Bool,
  _settingVerbose  :: Bool
  } deriving (Eq, Show)

makeLenses ''Settings

options :: [OptDescr (Settings -> Settings)]
options =
  [
    Option ['m'] ["dump-metadata"] (NoArg (& settingMetadata .~ True)) "Dump deduced metadata about the file",
    Option ['v'] ["verbose"] (NoArg (& settingVerbose .~ True)) "Print extra debugging information on stderr"
  ]

defaultSettings :: Settings
defaultSettings = Settings {
  _settingFilename = error "defaultSettings: Must provide a filename",
  _settingMetadata = False,
  _settingVerbose  = False
  }

parseOptions :: [String] -> IO ([Settings -> Settings])
parseOptions args =
  case getOpt Permute options args of
    (opts, filename:[], []) -> return $ (&settingFilename .~ filename):opts
    (_, [], [])             -> fail $ "Must provide a filename\n" ++ usageInfo header options
    (_, _, errors)          -> fail $ concat errors ++ usageInfo header options
  where
    header = "Usage: csv2yxdb [OPTIONS...] filename"

processOptions :: [Settings -> Settings] -> Settings
processOptions = foldl (flip ($)) defaultSettings

getSettings :: IO Settings
getSettings = do
  argv <- getArgs
  opts <- parseOptions argv
  return $ processOptions opts

runMetadata :: StateT Settings IO ()
runMetadata = undefined

runCsv2Yxdb :: StateT Settings IO ()
runCsv2Yxdb = undefined

main :: IO ()
main = do
  settings <- getSettings
  flip evalStateT settings $
    case () of
      _ | settings ^. settingMetadata -> runMetadata
        | otherwise                   -> runCsv2Yxdb
