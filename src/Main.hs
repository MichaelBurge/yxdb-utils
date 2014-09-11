{-# LANGUAGE TemplateHaskell #-}

import Database.Alteryx

import Control.Applicative
import Control.Lens
import Control.Monad.Trans.Resource
import qualified Data.ByteString as BS
import Data.Conduit
import Data.Conduit.Binary
import System.Console.GetOpt
import System.Environment
import System.IO

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

getSource :: Settings -> Source (ResourceT IO) BS.ByteString
getSource settings =
  case settings ^. settingFilename of
    Just filename -> sourceFile filename
    Nothing       -> sourceHandle stdin

runYxdb2Csv :: Settings -> IO ()
runYxdb2Csv settings =
  runResourceT $
    getSource settings $=
    bytes2yxdb =$=
    yxdb2csv =$=
    csv2bytes $$
    sinkHandle stdout

main = runYxdb2Csv <$> getSettings
