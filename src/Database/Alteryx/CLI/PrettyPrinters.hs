{-# LANGUAGE OverloadedStrings #-}

module Database.Alteryx.CLI.PrettyPrinters
  (
    printRecordInfo
  ) where

import Database.Alteryx.Types

import Control.Lens
import Control.Newtype as NT
import Data.Monoid
import Data.Text as T
import Data.Text.IO
import Prelude hiding (putStrLn)

printRecordInfo :: RecordInfo -> IO ()
printRecordInfo recordInfo =
  let printField field =
        putStrLn $ (
          "  " <> (field ^. fieldName) <> ": "
          <> (T.pack $ show $ field ^. fieldType)
          <> case field ^. fieldSize of
            Just x -> " - Size: " <> (T.pack $ show x)
            Nothing -> ""
          <> case field ^. fieldScale of
            Just x -> " - Scale: " <> (T.pack $ show x)
            Nothing -> ""
         :: Text )
  in do
    putStrLn "RecordInfo:"
    mapM_ printField $ NT.unpack recordInfo
