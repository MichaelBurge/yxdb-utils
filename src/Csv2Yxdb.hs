{-# LANGUAGE TemplateHaskell,OverloadedStrings #-}

import Database.Alteryx

import Control.Lens
import Control.Monad.State
import Control.Monad.Trans.Resource
import Data.Conduit as C
import Data.Conduit.Binary as C
import Data.Conduit.List as CL
import Data.Conduit.Text as CT
import Data.Text as T hiding (null, foldl, head)
import System.Console.GetOpt
import System.Environment
import Text.ParserCombinators.Parsec

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
    (_, _, errors)          -> fail $ Prelude.concat errors ++ usageInfo header options
  where
    header = "Usage: csv2yxdb [OPTIONS...] filename"

processOptions :: [Settings -> Settings] -> Settings
processOptions = Prelude.foldl (flip ($)) defaultSettings

getSettings :: IO Settings
getSettings = do
  argv <- getArgs
  opts <- parseOptions argv
  return $ processOptions opts

runMetadata :: StateT Settings IO ()
runMetadata = do
  settings <- get
  let filename = settings ^. settingFilename
  mLine <- runResourceT $
             sourceFile filename =$=
             decode utf8 =$=
             CT.lines $$
             CL.head
  case mLine of
    Nothing   -> liftIO $ putStrLn "Cannot convert an empty file"
    Just line -> liftIO $ do
      let eRecordInfo = parse parseCSVHeader filename $ T.unpack line
      case eRecordInfo of
        Left e           -> putStrLn $ show e
        Right recordInfo -> printRecordInfo recordInfo

runCsv2Yxdb :: StateT Settings IO ()
runCsv2Yxdb = undefined

main :: IO ()
main = do
  settings <- getSettings
  flip evalStateT settings $
    case () of
      _ | settings ^. settingMetadata -> runMetadata
        | otherwise                   -> runCsv2Yxdb
