{-# LANGUAGE OverloadedStrings #-}

module Database.Alteryx.CSVConversion
    (
     csv2bytes,
     parseCSVHeader,
     record2csv
    ) where

import Control.Applicative
import Control.Lens
import Control.Monad.Catch hiding (try)
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
import Text.Parsec.Char
import Text.Parsec.Combinator
import qualified Text.Parsec.Language as Language
import qualified Text.Parsec.Token as Token
import Text.ParserCombinators.Parsec

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

lexer = Token.makeTokenParser Language.emptyDef

identifier = Token.identifier lexer
integer    = Token.integer lexer
natural    = Token.natural lexer
symbol     = Token.symbol lexer

parseFieldType :: Parser (Field -> Field)
parseFieldType =
  let parseParens = between (symbol "(") (symbol ")")
      parseOneArg = parseParens integer
      parseTwoArgs = parseParens $ do
        arg1 <- fromInteger <$> integer
        _ <- symbol ","
        arg2 <- fromInteger <$> integer
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
      symbol ":" *> parseFieldType,
                    return id
      ]
    return $ applyParameters $
             defaultField & fieldName .~ T.pack name

parseCSVHeader :: Parser RecordInfo
parseCSVHeader = RecordInfo <$> parseCSVHeaderField `sepBy` symbol "|"
