{-# LANGUAGE OverloadedStrings #-}

module Database.Alteryx.CSVConversion
    (
     csv2bytes,
     parseCSVHeader,
     parseCSVRecord,
     parseCSVRecords,
     record2csv
    ) where

import Control.Applicative
import Control.Lens
import Control.Monad.Catch hiding (try)
import qualified Control.Newtype as NT
import Data.Attoparsec.Text as AT
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
    try $ string "date"     *> return (& fieldType .~ FTDate),
    try $ string "time"     *> return (& fieldType .~ FTTime),
    try $ string "datetime" *> return (& fieldType .~ FTDateTime),
    try $ string "blob"     *> parseSize FTBlob,
    try $ string "spatial"  *> parseSize FTBlob,
    try $ string "unknown"  *> return (& fieldType .~ FTUnknown)
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
parseCSVHeader = RecordInfo <$> parseCSVHeaderField `sepBy` char '|'

parseCSVField :: Field -> Parser (Maybe FieldValue)
parseCSVField field = do
  c <- peekChar
  case c of
    Nothing -> return Nothing
    Just _ -> Just <$> case field ^. fieldType of
      FTBool          -> error "parseCSVField: Bool unimplemented"
      FTByte          -> FVInt16 <$> decimal
      FTInt16         -> FVInt16 <$> decimal
      FTInt32         -> FVInt16 <$> decimal
      FTInt64         -> FVInt16 <$> decimal
      FTFixedDecimal  -> error "parseCSVField: FixedDecimal unimplemented"
      FTFloat         -> FVFloat <$> rational
      FTDouble        -> FVDouble <$> rational
      FTString        -> FVString <$> takeText
      FTWString       -> FVWString <$> takeText
      FTVString       -> FVVString <$> takeText
      FTVWString      -> FVVWString <$> takeText
      FTDate          -> error "parseCSVField: Date unimplemented"
      FTTime          -> error "parseCSVField: Time unimplemented"
      FTDateTime      -> error "parseCSVField: DateTime unimplemented"
      FTBlob          -> error "parseCSVField: Blob unimplemented"
      FTSpatialObject -> error "parseCSVField: Spatial Object unimplemented"
      FTUnknown       -> error "parseCSVField: Unknown unimplemented"    

parseCSVRecord :: RecordInfo -> Parser Record
parseCSVRecord (RecordInfo fields) =
  let parseFields :: [Field] -> Parser ([Maybe FieldValue])
      parseFields [] = fail "No fields"
      parseFields [f] = return <$> parseCSVField f
      parseFields (f:fs) = do
        text <- AT.takeWhile (\c -> c /= '|' && c /= '\n')
        let eFV = parseOnly (parseCSVField f) text
        case eFV of
          Left e -> error $ show e
          Right fv -> do
            fvs <- parseFields fs
            return $ fv:fvs
  in NT.pack <$> mapM parseCSVField fields

parseCSVRecords :: RecordInfo -> Parser [Record]
parseCSVRecords recordInfo = takeText >>= (\t -> return [Record [Just $ FVString t]])
