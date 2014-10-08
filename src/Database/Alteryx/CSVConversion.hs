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
import Data.Attoparsec.Text as AT
import Data.ByteString as BS
import Data.ByteString.Char8 as BSC
import Data.Conduit
import Data.Conduit.Binary as CB
import Data.Conduit.Attoparsec as CP
import Data.Conduit.Combinators as CC
import Data.Conduit.List as CL hiding (isolate)
import Data.Conduit.Text as CT
import Data.Maybe
import Data.Monoid
import qualified Data.CSV.Conduit as CSVT
import qualified Data.CSV.Conduit.Parser.Text as CSVT
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Builder as TB
import qualified Data.Text.Lazy.Builder.Int as TB
import qualified Data.Text.Lazy.Builder.RealFloat as TB

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
    Just (FVFloat f)    -> TB.formatRealFloat TB.Fixed (Just 4) f
    Just (FVDouble f)   -> TB.formatRealFloat TB.Fixed (Just 4) f
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

between :: Parser a -> Parser a -> Parser b -> Parser b
between left right middle = do
  _ <- left
  x <- middle
  _ <- right
  return x

parseFieldType :: Parser (Field -> Field)
parseFieldType =
  let parseParens = between (char '(') (char ')')
      parseOneArg = parseParens decimal
      parseTwoArgs = parseParens $ do
        arg1 <- decimal
        _    <- char ','
        arg2 <- decimal
        return (arg1, arg2)
      parseSize fType = do
        size <- fromInteger <$> parseOneArg
        return $ \field -> field & fieldType .~ fType
                                 & fieldSize .~ Just size
  in choice [
    try $ string "bool"    *> return (& fieldType .~ FTBool),
    try $ string "int(8)"  *> return (& fieldType .~ FTByte),
    try $ string "int(16)" *> return (& fieldType .~ FTInt16),
    try $ string "int(32)" *> return (& fieldType .~ FTInt32),
    try $ string "int(64)" *> return (& fieldType .~ FTInt64),
    try $ string "decimal" *> do
      (size, scale) <- parseTwoArgs
      return $ \field -> field & fieldType  .~ FTFixedDecimal
                               & fieldSize  .~ Just size
                               & fieldScale .~ Just scale,
    try $ string "float"    *> return (& fieldType .~ FTFloat),
    try $ string "double"   *> return (& fieldType .~ FTDouble),
    try $ string "string"   *> parseSize FTString,
    try $ string "wstring"  *> parseSize FTWString,
    try $ string "vstring"  *> parseSize FTVString,
    try $ string "vwstring" *> parseSize FTVWString,
    try $ string "datetime" *> return (& fieldType .~ FTDateTime),
    try $ string "date"     *> return (& fieldType .~ FTDate),
    try $ string "time"     *> return (& fieldType .~ FTTime),
    try $ string "blob"     *> parseSize FTBlob,
    try $ string "spatial"  *> parseSize FTBlob,
                               return (& fieldType .~ FTUnknown)
    ]

identifier :: Parser T.Text
identifier = T.pack <$> (many $ satisfy $ inClass "a-zA-Z0-9_")

parseCSVHeaderField :: Parser Field
parseCSVHeaderField =
  let defaultField = Field {
        _fieldName  = error "No name",
        _fieldType  = FTUnknown,
        _fieldSize  = Nothing,
        _fieldScale = Nothing
      }
  in do
    name <- identifier
    applyParameters <- choice [
      char ':' *> parseFieldType,
                  return id
      ]
    return $ applyParameters $
             defaultField & fieldName .~ name

parseCSVHeader :: Parser RecordInfo
parseCSVHeader = RecordInfo <$> (parseCSVHeaderField `sepBy` char '|' )

parseCSVField :: Field -> Parser (Maybe FieldValue)
parseCSVField field = do
  c <- peekChar
  case c of
    Nothing -> return Nothing
    Just _ -> Just <$> case field ^. fieldType of
      FTBool          -> error "parseCSVField: Bool unimplemented"
      FTByte          -> FVByte <$> decimal
      FTInt16         -> FVInt16 <$> decimal
      FTInt32         -> FVInt32 <$> decimal
      FTInt64         -> FVInt64 <$> decimal
      FTFixedDecimal  -> FVString <$> takeText -- TODO: Wrong!
      FTFloat         -> FVFloat <$> rational
      FTDouble        -> FVDouble <$> rational
      FTString        -> FVString <$> takeText
      FTWString       -> FVWString <$> takeText
      FTVString       -> FVVString <$> takeText
      FTVWString      -> FVVWString <$> takeText
      FTDate          -> FVString <$> takeText -- TODO: Wrong!
      FTTime          -> FVString <$> takeText -- TODO: Wrong!
      FTDateTime      -> FVString <$> takeText -- TODO: Wrong!
      FTBlob          -> error "parseCSVField: Blob unimplemented"
      FTSpatialObject -> error "parseCSVField: Spatial Object unimplemented"
      FTUnknown       -> error "parseCSVField: Unknown unimplemented"

csvHunks2records :: (MonadThrow m) => RecordInfo -> Conduit [T.Text] m Record
csvHunks2records recordInfo@(RecordInfo fields) =
    let numFields = Prelude.length fields
    in do
      mRow <- await
      case mRow of
        Nothing -> return ()
        Just [] -> return ()
        Just columns -> do
            let eFieldValues =
                    zipWithM (\field column -> parseOnly (parseCSVField field) column)
                             fields
                             columns
            case eFieldValues of
              Left e -> fail e
              Right fieldValues -> do
                  yield $ Record fieldValues
                  csvHunks2records recordInfo

csv2csvHunks :: (MonadThrow m) => CSVT.CSVSettings -> Conduit T.Text m [T.Text]
csv2csvHunks csvSettings =
    CL.map (\x -> T.snoc x '\n') =$=
    CP.conduitParser (CSVT.row csvSettings) =$=
    CL.map snd =$=
    CL.catMaybes

csv2records :: (MonadThrow m) => CSVT.CSVSettings -> Conduit T.Text m Record
csv2records csvSettings = CT.lines =$= do
  mHeader <- await
  case mHeader of
    Nothing -> return ()
    Just header -> do
      let eRecordInfo = parseOnly parseCSVHeader header
      case eRecordInfo of
        Left e -> error e
        Right recordInfo ->
          csv2csvHunks csvSettings =$=
          csvHunks2records recordInfo


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
  in field ^. fieldName <> ":" <> typeIndicator <> "\n"


-- | The appropriate CSV header that describes a record. Example: "month:date|market:int(16)|num_households:int(32)"
csvHeader :: RecordInfo -> T.Text
csvHeader (RecordInfo fields) = T.intercalate "|" $ Prelude.map csvHeaderField fields

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
