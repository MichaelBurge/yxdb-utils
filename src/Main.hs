{-# LANGUAGE TemplateHaskell #-}

import Database.Alteryx

import Control.Lens
import System.Console.GetOpt
import System.Environment

data Settings = Settings {
  _settingVerbose  :: Bool,
  _settingFilename :: Maybe String
  }

makeLenses ''Settings

options :: [OptDescr (Settings -> Settings)]
options =
  [
    Option ['v'] ["verbose"] (NoArg (& settingVerbose .~ True)) "Print extra debugging information on stderr"
  ]

defaultSettings = Settings {
  _settingVerbose = False,
  _settingFilename = Nothing
  }

parseOptions :: [String] -> IO ([Settings -> Settings])
parseOptions args =
  case getOpt Permute options args of
    (opts, filename:[], []) -> return $ (\o -> o & settingFilename .~ Just filename):opts
    (opts, [], [])          -> return opts
    (_, _, errors)          -> fail $ concat errors ++ usageInfo header options
  where
    header = "Usage: csv2yxdb [OPTIONS...] filename"

processOptions :: [Settings -> Settings] -> Settings
processOptions = foldl (flip ($)) defaultSettings

getSettings :: IO Settings
getSettings = do
  argv <- getArgs
  options <- parseOptions argv
  return $ processOptions options

main = putStrLn "hello"
