{-# LANGUAGE OverloadedStrings #-}

module Database.Alteryx.CSVConversion
    (
     csv2bytes,
     record2csv
    ) where

import Control.Monad.Catch
import qualified Control.Newtype as NT
import Data.ByteString as BS
import Data.Conduit
import Data.Conduit.Text
import Data.Monoid
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Builder as TB
import qualified Data.Text.Lazy.Builder.Int as TB
import qualified Data.Text.Lazy.Builder.RealFloat as TB

import Database.Alteryx.Serialization()
import Database.Alteryx.Types

csv2bytes :: MonadThrow m => Conduit T.Text m BS.ByteString
csv2bytes = encode utf8

record2csv :: Monad m => Conduit Record m T.Text
record2csv = do
  mRecord <- await
  case mRecord of
    Just record -> do
      let line = T.intercalate "|" $
                 Prelude.map (TL.toStrict . TB.toLazyText . renderFieldValue) $
                 NT.unpack record
      yield $ line `mappend` "\n"
      record2csv
    Nothing -> return ()

renderFieldValue :: Maybe FieldValue -> TB.Builder
renderFieldValue fieldValue =
  -- TODO: Floating point values need to get their size information from the metadata
  case fieldValue of
    Just (FVDouble f) -> TB.formatRealFloat TB.Fixed (Just 4) f
    Just (FVInt16 x)  -> TB.decimal x
    Just (FVInt32 x)  -> TB.decimal x
    Just (FVInt64 x)  -> TB.decimal x
    Just (FVString x) -> TB.fromText x
    Nothing           -> TB.fromText ""
    _                 -> error $ "renderFieldValue: Unlisted case: " ++ show fieldValue
