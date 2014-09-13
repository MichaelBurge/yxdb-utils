{-# LANGUAGE TemplateHaskell,OverloadedStrings #-}

import Database.Alteryx

import Control.Applicative
import Control.Lens
import Control.Monad
import Control.Monad.State
import Control.Monad.Trans.Resource
import qualified Control.Newtype as NT
import Data.Array.Unboxed as A
import qualified Data.ByteString as BS
import Data.Conduit
import Data.Conduit.Binary
import Data.Int
import Data.Monoid
import Data.Text as T hiding (concat, foldl)
import Data.Text.IO
import Prelude hiding (putStrLn)
import System.Console.GetOpt
import System.Environment
import System.IO hiding (putStrLn)

data Settings = Settings {
  _settingMetadata :: Bool,
  _settingVerbose  :: Bool,
  _settingFilename :: String
  }

makeLenses ''Settings

options :: [OptDescr (Settings -> Settings)]
options =
  [
    Option ['m'] ["dump-metadata"] (NoArg (& settingMetadata .~ True)) "Dump the file's metadata",
    Option ['v'] ["verbose"] (NoArg (& settingVerbose .~ True)) "Print extra debugging information on stderr"
  ]

defaultSettings = Settings {
  _settingMetadata = False,
  _settingVerbose  = False,
  _settingFilename = error "defaultSettings: Filename empty"
  }

parseOptions :: [String] -> IO ([Settings -> Settings])
parseOptions args =
  case getOpt Permute options args of
    (opts, filename:[], []) -> return $ (\o -> o & settingFilename .~ filename):opts
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

printRecordInfo :: YxdbMetadata -> StateT Settings IO ()
printRecordInfo metadata =
  let printField field =
        putStrLn $ "  " <>
                   (field ^. fieldName) <> ": " <>
                   (T.pack $ show $ field ^. fieldType)
  in liftIO $ do
     putStrLn "RecordInfo:"
     Prelude.mapM_ printField $ NT.unpack $ metadata ^. metadataRecordInfo

runMetadata :: StateT Settings IO ()
runMetadata = do
  settings <- get
  yxdbMetadata <- liftIO $ getMetadata $ settings ^. settingFilename
  printHeader yxdbMetadata
  printRecordInfo yxdbMetadata
  when (settings ^. settingVerbose) $
    printBlocks yxdbMetadata


runYxdb2Csv :: StateT Settings IO ()
runYxdb2Csv = do
  settings <- get
  let filename = settings ^. settingFilename
  metadata <- liftIO $ getMetadata filename
  runResourceT $
    yieldNBlocks 1 filename metadata $=
    streamRecords metadata =$=
    record2csv =$=
    csv2bytes $$
    sinkHandle stdout

main :: IO ()
main = do
  settings <- getSettings
  flip evalStateT settings $
    case () of
      _ | settings ^. settingMetadata -> runMetadata
        | otherwise                   -> runYxdb2Csv
