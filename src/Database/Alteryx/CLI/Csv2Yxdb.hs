{-# LANGUAGE TemplateHaskell,OverloadedStrings #-}
module Database.Alteryx.CLI.Csv2Yxdb where

import Database.Alteryx

import Control.Applicative
import Control.Lens
import Control.Monad.State
import Control.Monad.Trans.Resource
import Data.Attoparsec.Text
import Data.Conduit as C
import Data.Conduit.Binary as C
import Data.Conduit.List as CL
import Data.Conduit.Text as CT
import qualified Data.CSV.Conduit as CSVT
import qualified Data.CSV.Conduit.Parser.Text as CSVT
import Data.Maybe
import Data.Monoid
import Data.Text as T hiding (null, foldl, head)
import System.Console.GetOpt
import System.Environment
import System.IO hiding (putStrLn, utf8)

data Settings = Settings {
  _settingHeader   :: Maybe T.Text,
  _settingFilename :: FilePath,
  _settingOutput   :: FilePath,
  _settingCSV      :: CSVT.CSVSettings,
  _settingInternal :: Bool,
  _settingMetadata :: Bool,
  _settingVerbose  :: Bool
  } deriving (Eq, Show)

makeLenses ''Settings

options :: [OptDescr (Settings -> Settings)]
options =
  [
   Option ['h'] ["header"] (ReqArg (\o -> (& settingHeader .~ Just (T.pack o))) "Header line") "If you'd prefer not to include header lines in the CSV file, you can provide it on the command line",
   Option ['o'] ["output"] (ReqArg (\o -> (& settingOutput .~ o)) "Output filename" ) "Name of the output file",
   Option ['i'] ["dump-internal"] (NoArg (& settingInternal .~ True)) "Dump internal representation of parsed records",
   Option ['m'] ["dump-metadata"] (NoArg (& settingMetadata .~ True)) "Dump deduced metadata about the file",
   Option ['v'] ["verbose"] (NoArg (& settingVerbose .~ True)) "Print extra debugging information on stderr"
  ]

defaultSettings :: Settings
defaultSettings = Settings {
  _settingHeader   = Nothing,
  _settingFilename = error "defaultSettings: Must provide a filename",
  _settingOutput   = error "defaultsettings: Must provide an output file",
  _settingCSV      = CSVT.defCSVSettings { CSVT.csvSep = '|', CSVT.csvQuoteChar = Nothing },
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
getRecordInfo =
  let readHeaderFromFile = do
        settings <- get
        let filename = settings ^. settingFilename
        mLine <- runResourceT $
                   sourceFile filename =$=
                   decode utf8 =$=
                   CT.lines $$
                   CL.head
        return mLine
  in do
    settings <- get
    mLine <- case settings ^. settingHeader of
               Nothing -> readHeaderFromFile
               Just x -> return $ Just x
    case mLine of
      Nothing   -> do
        liftIO $ putStrLn "No header found"
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

prependHeader :: T.Text -> Conduit T.Text (ResourceT IO) T.Text
prependHeader header = do
  yield $ header <> "\n"
  CL.map id

getRecordSource :: StateT Settings IO (Source (ResourceT IO) Record)
getRecordSource = do
  settings <- get
  let filename = settings ^. settingFilename
  let maybePrependHeader = case settings ^. settingHeader of
                             Nothing -> CL.map id
                             Just x  -> prependHeader x
  return $
    sourceFile filename =$=
    decode utf8 $=
    maybePrependHeader =$=
    csv2records (settings ^. settingCSV)

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
  recordInfo   <- fromJust <$> getRecordInfo
  recordSource <- getRecordSource
  settings     <- get
  liftIO $
      withBinaryFile (settings ^. settingOutput) WriteMode $ \ h -> do
                runResourceT $
                  recordSource $$
                  sinkRecords h recordInfo

csv2yxdbMain :: IO ()
csv2yxdbMain = do
  settings <- getSettings
  flip evalStateT settings $
    case () of
      _ | settings ^. settingMetadata -> runMetadata
        | settings ^. settingInternal -> runCsv2Internal
        | otherwise                   -> runCsv2Yxdb
