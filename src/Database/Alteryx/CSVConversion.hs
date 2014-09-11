module Database.Alteryx.CSVConversion
    (
     bytes2yxdb,
     csv2bytes,
     yxdb2csv
    ) where

import Control.Applicative
import Control.Lens
import Control.Monad.Catch
import qualified Control.Newtype as NT
import Data.ByteString as BS
import Data.Conduit
import Data.Conduit.Combinators as CC
import Data.Conduit.Serialization.Binary
import Data.Conduit.Text
import Data.Monoid
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Builder as TB
import qualified Data.Text.Lazy.Builder.RealFloat as TB

import Database.Alteryx.Serialization
import Database.Alteryx.Types

bytes2yxdb :: MonadThrow m => Conduit BS.ByteString m YxdbFile
bytes2yxdb = conduitDecode

csv2bytes :: MonadThrow m => Conduit T.Text m BS.ByteString
csv2bytes = encode utf8

yxdb2csv :: Monad m => Conduit YxdbFile m T.Text
yxdb2csv = do
  mYxdbFile <- await
  case mYxdbFile of
    Just yxdbFile ->
      let recordSource = yieldMany $ yxdbFile ^. records
      in do
        builder <- recordSource $= record2csvBuilder $$ fold
        yield $ TL.toStrict $ TB.toLazyText $ builder
        yxdb2csv
    Nothing -> return ()

record2csvBuilder :: Monad m => Conduit Record m TB.Builder
record2csvBuilder = do
  mRecord <- await
  case mRecord of
    Just record -> do
      let bLine = mconcat $
                     Prelude.map renderFieldValue $
                     NT.unpack record
      yield bLine
      record2csvBuilder
    Nothing -> return ()

renderFieldValue :: FieldValue -> TB.Builder
renderFieldValue fieldValue =
  -- TODO: Floating point values need to get their size information from the metadata
  case fieldValue of
    FVDouble f -> TB.formatRealFloat TB.Fixed (Just 4) f
