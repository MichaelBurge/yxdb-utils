{-# LANGUAGE TemplateHaskell,OverloadedStrings #-}

import Database.Alteryx

import Control.Applicative
import Control.Lens
import Control.Monad.State
import Control.Monad.Trans.Resource
import Data.Attoparsec.Text
import Data.Conduit as C
import Data.Conduit.Attoparsec as CP
import Data.Conduit.Binary as C
import Data.Conduit.List as CL
import Data.Conduit.Text as CT
import Data.Maybe
import Data.Text as T hiding (null, foldl, head)
import System.Console.GetOpt
import System.Environment
import System.IO hiding (putStrLn, utf8)

data Settings = Settings {
  _settingFilename :: String,
  _settingInternal :: Bool,
  _settingMetadata :: Bool,
  _settingVerbose  :: Bool
  } deriving (Eq, Show)

makeLenses ''Settings

options :: [OptDescr (Settings -> Settings)]
options =
  [
    Option ['i'] ["dump-internal"] (NoArg (& settingInternal .~ True)) "Dump internal representation of parsed records",
    Option ['m'] ["dump-metadata"] (NoArg (& settingMetadata .~ True)) "Dump deduced metadata about the file",
    Option ['v'] ["verbose"] (NoArg (& settingVerbose .~ True)) "Print extra debugging information on stderr"
  ]

defaultSettings :: Settings
defaultSettings = Settings {
  _settingFilename = error "defaultSettings: Must provide a filename",
  _settingInternal = False,
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

getRecordInfo :: StateT Settings IO (Maybe RecordInfo)
getRecordInfo = do
  settings <- get
  let filename = settings ^. settingFilename
  mLine <- runResourceT $
             sourceFile filename =$=
             decode utf8 =$=
             CT.lines $$
             CL.head
  case mLine of
    Nothing   -> do
      liftIO $ putStrLn "Cannot convert an empty file"
      return Nothing
    Just line -> liftIO $ do
      let eRecordInfo = parseOnly parseCSVHeader line
      case eRecordInfo of
        Left e           -> do
          liftIO $ putStrLn $ show e
          return Nothing
        Right recordInfo -> return $ Just recordInfo

runMetadata :: StateT Settings IO ()
runMetadata = do
  mRecordInfo <- getRecordInfo
  case mRecordInfo of
    Nothing -> return ()
    Just recordInfo -> liftIO $ printRecordInfo recordInfo

getRecordSource :: StateT Settings IO (Source (ResourceT IO) [Record])
getRecordSource = do
  mRecordInfo <- getRecordInfo
  settings <- get
  let filename = settings ^. settingFilename
  case mRecordInfo of
    Nothing -> return $ return ()
    Just recordInfo ->
      return $
        sourceFile filename $=
        decode utf8 $=
        CP.conduitParser (parseCSVRecords recordInfo) =$=
        CL.map snd

runCsv2Internal :: StateT Settings IO ()
runCsv2Internal = do
  recordSource <- getRecordSource
  liftIO $ runResourceT $
    recordSource $=
    CL.map (T.pack . show) =$=
    encode utf8 $$
    sinkHandle stdout

runCsv2Yxdb :: StateT Settings IO ()
runCsv2Yxdb = do
  recordInfo <- fromJust <$> getRecordInfo
  recordSource <- getRecordSource
  liftIO $ runResourceT $
    recordSource $=
    CL.concat =$=
    recordsToBlocks recordInfo =$=
    blocksToYxdbBytes recordInfo $$
    sinkHandle stdout

main :: IO ()
main = do
  settings <- getSettings
  flip evalStateT settings $
    case () of
      _ | settings ^. settingMetadata -> runMetadata
        | settings ^. settingInternal -> runCsv2Internal
        | otherwise                   -> runCsv2Yxdb
