{-# LANGUAGE OverloadedStrings #-}

module Database.Alteryx.CSVConversion
    (
     alteryxCsvSettings,
     csv2bytes,
     csv2records,
     parseCSVHeader,
     record2csv,
     sourceCsvRecords
    ) where

import Control.Applicative
import Control.Lens
import Control.Monad
import Control.Monad.Catch hiding (try)
import Control.Monad.Trans.Resource
import qualified Control.Newtype as NT
import qualified Data.Attoparsec.Text as AT
import Data.ByteString as BS
import Data.ByteString.Char8 as BSC
import Data.Conduit
import Data.Conduit.Binary as CB
import Data.Conduit.Attoparsec as CP
import Data.Conduit.Combinators as CC
import Data.Conduit.List as CL hiding (isolate)
import Data.Conduit.Text as CT
import Data.Either.Combinators
import Data.Maybe
import Data.Monoid
import qualified Data.CSV.Conduit as CSVT
import qualified Data.CSV.Conduit.Parser.Text as CSVT
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Builder as TB
import qualified Data.Text.Lazy.Builder.Int as TB
import qualified Data.Text.Lazy.Builder.RealFloat as TB
import qualified Data.Text.Read as T

import Database.Alteryx.Serialization()
import Database.Alteryx.Types

-- | Our CSVs are pipe('|')-separated and do not do quoting.
alteryxCsvSettings :: CSVT.CSVSettings
alteryxCsvSettings = CSVT.defCSVSettings { CSVT.csvSep = '|', CSVT.csvQuoteChar = Nothing }

-- | All CSV text should be UTF-8 encoded.
csv2bytes :: MonadThrow m => Conduit T.Text m BS.ByteString
csv2bytes = encode utf8

record2csv :: (MonadResource m) => RecordInfo -> Conduit Record m T.Text
record2csv recordInfo =
  let record2csvWithoutHeader = do
        mRecord <- await
        case mRecord of
          Just record -> do
            let line = T.intercalate "|" $
                       Prelude.map (TL.toStrict . TB.toLazyText . renderFieldValue) $
                       NT.unpack record
            yield $ line `mappend` "\n"
            record2csvWithoutHeader
          Nothing -> return ()
  in do
    yield $ csvHeader recordInfo
    record2csvWithoutHeader

renderFieldValue :: Maybe FieldValue -> TB.Builder
renderFieldValue fieldValue =
  -- TODO: Floating point values need to get their size information from the metadata
  case fieldValue of
    Just (FVFloat f)    -> TB.formatRealFloat TB.Fixed Nothing f
    Just (FVDouble f)   -> TB.formatRealFloat TB.Fixed Nothing f
    Just (FVByte x)     -> TB.decimal x
    Just (FVInt16 x)    -> TB.decimal x
    Just (FVInt32 x)    -> TB.decimal x
    Just (FVInt64 x)    -> TB.decimal x
    Just (FVString x)   -> TB.fromText x
    Just (FVWString x)  -> TB.fromText x
    Just (FVVString x)  -> TB.fromText x
    Just (FVVWString x) -> TB.fromText x
    Nothing           -> TB.fromText ""
    _                 -> error $ "renderFieldValue: Unlisted case: " ++ show fieldValue

between :: AT.Parser a -> AT.Parser a -> AT.Parser b -> AT.Parser b
between left right middle = do
  _ <- left
  x <- middle
  _ <- right
  return x

keyword :: T.Text -> AT.Parser T.Text
keyword text = (AT.try $ AT.string text)

parseFieldType :: AT.Parser (Field -> Field)
parseFieldType =
  let parseParens = between (AT.char '(') (AT.char ')')
      parseOneArg = parseParens AT.decimal
      parseTwoArgs = parseParens $ do
        arg1 <- AT.decimal
        _    <- AT.char ','
        arg2 <- AT.decimal
        return (arg1, arg2)
      parseSize fType = do
        size <- fromInteger <$> parseOneArg
        return $ \field -> field & fieldType .~ fType
                                 & fieldSize .~ Just size
  in AT.choice [
    AT.string "bool"    *> return (& fieldType .~ FTBool),
    AT.string "int(8)"  *> return (& fieldType .~ FTByte),
    AT.string "int(16)" *> return (& fieldType .~ FTInt16),
    AT.string "int(32)" *> return (& fieldType .~ FTInt32),
    AT.string "int(64)" *> return (& fieldType .~ FTInt64),
    AT.string "decimal" *> do
      (size, scale) <- parseTwoArgs
      return $ \field -> field & fieldType  .~ FTFixedDecimal
                               & fieldSize  .~ Just size
                               & fieldScale .~ Just scale,
    AT.string "float"    *> return (& fieldType .~ FTFloat),
    AT.string "double"   *> return (& fieldType .~ FTDouble),
    AT.string "string"   *> parseSize FTString,
    AT.string "wstring"  *> parseSize FTWString,
    AT.string "vstring"  *> parseSize FTVString,
    AT.string "vwstring" *> parseSize FTVWString,
    AT.string "datetime" *> return (& fieldType .~ FTDateTime),
    AT.string "date"     *> return (& fieldType .~ FTDate),
    AT.string "time"     *> return (& fieldType .~ FTTime),
    AT.string "blob"     *> parseSize FTBlob,
    AT.string "spatial"  *> parseSize FTBlob
    ]
  AT.<?> "parseFieldType"

identifier :: AT.Parser T.Text
identifier = AT.takeWhile1 (AT.inClass "a-zA-Z0-9_")

parseCSVHeaderField :: AT.Parser Field
parseCSVHeaderField =
  let defaultField = Field {
        _fieldName  = error "No name",
        _fieldType  = error "No type",
        _fieldSize  = Nothing,
        _fieldScale = Nothing
      }
  in do
    name <- identifier
    AT.char ':'
    setType <- parseFieldType
    return $ setType $
           defaultField & fieldName .~ name

parseCSVHeader :: AT.Parser RecordInfo
parseCSVHeader = (RecordInfo <$> (parseCSVHeaderField `AT.sepBy1'` AT.char '|' )) <* AT.endOfInput

parseCSVField :: Field -> T.Text -> Maybe FieldValue
parseCSVField field text = do
  if T.null text
     then Nothing
     else Just $ case field ^. fieldType of
       FTBool          -> error "parseCSVField: Bool unimplemented"
       FTByte          -> FVByte $ fst $ fromRight' $ T.signed T.decimal $ text
       FTInt16         -> FVInt16 $ fst $ fromRight' $ T.signed T.decimal $ text
       FTInt32         -> FVInt32 $ fst $ fromRight' $ T.signed T.decimal $ text
       FTInt64         -> FVInt64 $ fst $ fromRight' $ T.signed T.decimal $ text
       FTFixedDecimal  -> FVString text
       FTFloat         -> FVFloat $ fst $ fromRight' $ T.rational text
       FTDouble        -> FVDouble $ fst $ fromRight' $ T.rational text
       FTString        -> FVString text
       FTWString       -> FVWString text
       FTVString       -> FVVString text
       FTVWString      -> FVVWString text
       FTDate          -> FVString text
       FTTime          -> FVString text
       FTDateTime      -> FVString text
       FTBlob          -> error "parseCSVField: Blob unimplemented"
       FTSpatialObject -> error "parseCSVField: Spatial Object unimplemented"
       FTUnknown       -> error "parseCSVField: Unknown unimplemented"

csvRow2Record :: RecordInfo -> T.Text -> Record
csvRow2Record (RecordInfo fields) columns =
    Record $ Prelude.zipWith parseCSVField fields
           $ T.splitOn "|" columns

csv2csvRecords :: (MonadThrow m) => RecordInfo -> Conduit T.Text m Record
csv2csvRecords recordInfo = do
  mLine <- await
  case mLine of
    Nothing -> return ()
    Just line -> do
      yield $ csvRow2Record recordInfo line
      csv2csvRecords recordInfo

csv2records :: (MonadThrow m) => CSVT.CSVSettings -> Conduit T.Text m Record
csv2records csvSettings = CT.lines =$= do
  mHeader <- await
  case mHeader of
    Nothing -> return ()
    Just header -> do
      let eRecordInfo = AT.parseOnly parseCSVHeader header
      case eRecordInfo of
        Left e -> error e
        Right recordInfo -> csv2csvRecords recordInfo

prependHeader :: (MonadResource m) => T.Text -> Conduit T.Text m T.Text
prependHeader header = do
  yield $ header <> "\n"
  CL.map id

csvHeaderField :: Field -> T.Text
csvHeaderField field =
  let renderSizeScale name = name <>
                             "(" <>
                             (T.pack $ show (fromJust $ field ^. fieldSize)) <>
                             "," <>
                             (T.pack $ show (fromJust $ field ^. fieldScale)) <>
                             ")"
      renderSize name = name <>
                        "(" <>
                        (T.pack $ show (fromJust $ field ^. fieldSize)) <>
                        ")"
      typeIndicator =
          case field ^. fieldType of
            FTBool          -> "bool"
            FTByte          -> "int(8)"
            FTInt16         -> "int(16)"
            FTInt32         -> "int(32)"
            FTInt64         -> "int(64)"
            FTFixedDecimal  -> renderSizeScale "decimal"
            FTFloat         -> "float"
            FTDouble        -> "double"
            FTString        -> renderSize "string"
            FTWString       -> renderSize "wstring"
            FTVString       -> "vstring"
            FTVWString      -> "vwstring"
            FTDateTime      -> "datetime"
            FTDate          -> "date"
            FTTime          -> "time"
            FTBlob          -> "blob"
            FTSpatialObject -> "spatial"
            FTUnknown       -> "unknown"
  in field ^. fieldName <> ":" <> typeIndicator


-- | The appropriate CSV header that describes a record. Example: "month:date|market:int(16)|num_households:int(32)"
csvHeader :: RecordInfo -> T.Text
csvHeader (RecordInfo fields) = T.snoc (T.intercalate "|" $ Prelude.map csvHeaderField fields) '\n'

-- | Stream the parsed records from a CSV file
sourceCsvRecords :: (MonadResource m) =>  FilePath -> Maybe T.Text -> CSVT.CSVSettings -> Source m Record
sourceCsvRecords filename header csvSettings =
  let maybePrependHeader = case header of
                             Nothing -> CL.map id
                             Just x  -> prependHeader x
  in CB.sourceFile filename =$=
     decode utf8 $=
     maybePrependHeader =$=
     csv2records csvSettings
