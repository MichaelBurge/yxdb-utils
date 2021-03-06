{-# LANGUAGE OverloadedStrings #-}

module Tests.Database.Alteryx (yxdbTests) where

import qualified Database.Alteryx.CLI.Csv2Yxdb as C2Y
import qualified Database.Alteryx.CLI.Yxdb2Csv as Y2C
import Database.Alteryx
import Tests.Database.Alteryx.Arbitrary
import Tests.Database.Utils

import Prelude hiding (readFile)

import Control.Applicative
import Control.Lens hiding (elements)
import Control.Monad (replicateM, when)
import Control.Monad.State (evalStateT)
import Control.Monad.Trans.Resource
import Data.Array.IArray (listArray)
import Data.Binary
import Data.Binary.Get (runGet)
import Data.Binary.Put (runPut)
import Data.ByteString as BS
import Data.ByteString.Char8 as BSC
import Data.ByteString.Lazy as BSL
import Data.Conduit
import Data.Conduit.Combinators
import Data.Conduit.Lift
import Data.Conduit.List as CL
import qualified Data.Conduit.Text as CT
import qualified Data.CSV.Conduit as CSVT
import Data.Text as T
import Data.Text.Encoding as T
import Data.Text.IO as T
import System.IO.Temp
import System.Process

import Test.Framework
import Test.Framework.Providers.HUnit (testCase)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.HUnit
import Test.QuickCheck

assertEq :: (Eq a, Show a) => a -> a -> Property
assertEq a b = let
    aStr = show a
    bStr = show b
    in printTestCase ("\nA: " ++ aStr ++ "\nB: " ++ bStr) (a == b)

assertDiffEqual :: T.Text -> T.Text -> Assertion
assertDiffEqual a b =
  if a == b
  then return ()
  else withSystemTempDirectory "x" $ \directory -> do
         let filenameA = directory ++ "/a"
             filenameB = directory ++ "/b"
         T.writeFile filenameA a
         T.writeFile filenameB b
         (code, diffOutput, diffError) <- readProcessWithExitCode "diff" [filenameA, filenameB] ""
         assertFailure $ "Diff failed: " ++ diffOutput

numBytes :: Binary a => a -> Int
numBytes x = fromIntegral $ BSL.length $ runPut $ put x

prop_HeaderLength :: Header -> Property
prop_HeaderLength header =
    assertEq headerPageSize (numBytes header)

prop_MetadataLength :: YxdbFile -> Property
prop_MetadataLength yxdb =
    assertEq (numMetadataBytesHeader $ yxdb ^. yxdbFileHeader)
             (numMetadataBytesActual $ yxdb ^. yxdbFileMetadata)

-- prop_BlocksLength :: YxdbFile -> Property
-- prop_BlocksLength yxdb =
--     assertEq (numBlocksBytesHeader $ header yxdb) (numBlocksBytesActual $ blocks yxdb)

prop_ValueGetAndPutAreInverses :: PairedValue -> Property
prop_ValueGetAndPutAreInverses (PairedValue field value) =
    assertEq (runGet (getValue field) (runPut $ putValue field value)) value

prop_BlockIndexGetAndPutAreInverses :: BlockIndex -> Property
prop_BlockIndexGetAndPutAreInverses x = assertEq (decode $ encode x) x

prop_MetadataGetAndPutAreInverses :: RecordInfo -> Property
prop_MetadataGetAndPutAreInverses x = assertEq (decode $ encode x) x

prop_HeaderGetAndPutAreInverses :: Header -> Property
prop_HeaderGetAndPutAreInverses x = assertEq (decode $ encode x) x

prop_BlocksGetAndPutAreInverses :: Block -> Property
prop_BlocksGetAndPutAreInverses x = assertEq (decode $ encode x) x

prop_YxdbFileGetAndPutAreInverses :: YxdbFile -> Property
prop_YxdbFileGetAndPutAreInverses x = assertEq (decode $ encode x) x

test_LoadingSmallModule :: Assertion
test_LoadingSmallModule = do
  parsed <- decodeFileOrFail "test-data/samples_of_various_field_types.yxdb"
  case parsed of
    Left (bytes, msg) -> assertFailure (msg ++ " at " ++ show bytes ++ " bytes")
    Right yxdbFile -> const (return ()) (yxdbFile :: YxdbFile)

csv2yxdb2csv :: C2Y.Settings -> FilePath -> FilePath -> IO T.Text
csv2yxdb2csv settings inputFilename outputFilename = do
  let c2ySettings = settings & C2Y.settingFilename .~ inputFilename
                             & C2Y.settingOutput .~ outputFilename
  let y2cSettings = Y2C.defaultSettings & Y2C.settingFilename .~ outputFilename
  evalStateT C2Y.runCsv2Yxdb c2ySettings
  newCsv <- T.decodeUtf8 <$> (captureStdout $ evalStateT Y2C.runYxdb2Csv y2cSettings)

  return newCsv

readCsvRecords :: Maybe Text -> FilePath -> IO [Record]
readCsvRecords header filename =
    runResourceT $
      sourceCsvRecords filename header alteryxCsvSettings $$
      sinkList

test_csv2yxdb2csvIsIdentity :: Assertion
test_csv2yxdb2csvIsIdentity = do
  let inputFilename = "test-data/samples.csv"
  let outputFilename = "test-data/samples.yxdb"
  newCsv <- csv2yxdb2csv C2Y.defaultSettings inputFilename outputFilename
  originalCsv <- T.readFile inputFilename
  assertEqual "CSV to YXDB to CSV" originalCsv newCsv

test_csv2yxdbQuoteParsing :: Assertion
test_csv2yxdbQuoteParsing = do
  let inputFilename = "test-data/quote-parsing.csv"
  let outputFilename = "test-data/quote-parsing.yxdb"
  oldCsv <- T.readFile inputFilename
  newCsv <- csv2yxdb2csv C2Y.defaultSettings inputFilename outputFilename
  assertEqual "" oldCsv newCsv

test_csv2yxdbDateFormat :: Assertion
test_csv2yxdbDateFormat = do
  let inputFilename = "test-data/date-format.csv"
      outputFilename = "test-data/date-format.yxdb"
  oldCsv <- T.readFile inputFilename
  newCsv <- csv2yxdb2csv C2Y.defaultSettings inputFilename outputFilename
  assertEqual "" oldCsv newCsv

nonAnsiCodepointFieldValue :: Maybe FieldValue
nonAnsiCodepointFieldValue = Just $ FVString "plátano"

nonAnsiCodepointField :: Field
nonAnsiCodepointField = Field {
                          _fieldName = "derp",
                          _fieldType = FTString,
                          _fieldSize = Just 10,
                          _fieldScale = Nothing
                        }

test_renderNonAnsiCodepoint :: Assertion
test_renderNonAnsiCodepoint = do
  let fieldValue = nonAnsiCodepointFieldValue
      field = nonAnsiCodepointField
      bs = runPut (putValue field fieldValue)
      parsedValue = runGet (getValue field) bs
  assertEqual "Converting a non-ANSI character lost information" fieldValue parsedValue

test_renderNonAnsiCodepointRecord :: Assertion
test_renderNonAnsiCodepointRecord = do
    let fieldValue = nonAnsiCodepointFieldValue
        field = nonAnsiCodepointField

        record = Record [ fieldValue ]
        recordInfo = RecordInfo [ field ]

        record2recordConduit = record2csv recordInfo =$=
                         csv2bytes =$=
                         CT.decode CT.utf8 =$=
                         csv2records alteryxCsvSettings
    newRecords <- runResourceT $ CL.sourceList [ record ] =$= record2recordConduit $$ sinkList

    let result = Prelude.head newRecords
    assertEqual "Converting a non-ANSI character lost information" record result

test_renderNonAnsiCodepointCSVRecord :: Assertion
test_renderNonAnsiCodepointCSVRecord =
  let csvText = "derp:string(10)\nplátano\n"
      recordInfo = RecordInfo [ nonAnsiCodepointField ]
      csv2csvConduit =
          csv2records alteryxCsvSettings =$=
          record2csv recordInfo =$=
          csv2bytes =$=
          CT.decode CT.utf8

  in do
    newCsv <- runResourceT $ CL.sourceList [ csvText ] =$= csv2csvConduit $$ sinkList
    assertEqual "" csvText $ T.concat newCsv

test_renderNonAnsiCodepointCSV2Record2Block2Record2CSV :: Assertion
test_renderNonAnsiCodepointCSV2Record2Block2Record2CSV =
  let csvText = "derp:string(10)\nplátano\n"
      recordInfo = RecordInfo [ nonAnsiCodepointField ]
      csv2csvConduit =
          csv2records alteryxCsvSettings =$=
          evalStateC defaultStatistics (recordsToBlocks recordInfo) =$=
          blocksToRecords recordInfo =$=
          record2csv recordInfo =$=
          csv2bytes =$=
          CT.decode CT.utf8
  in do
    newCsv <- runResourceT $ CL.sourceList [ csvText ] =$= csv2csvConduit $$ sinkList
    assertEqual "" csvText $ T.concat newCsv


test_recordParsing :: Assertion
test_recordParsing = do
  let inputFilename = "test-data/samples.csv"
      outputFilename = "test-data/samples.yxdb"
      secondOutputFilename = "test-data/samples.yxdb.csv"
      expectedRecord =
          [
           Record [
            Just (FVByte 1),
            Just (FVInt16 256),
            Just (FVInt32 32768),
            Just (FVInt64 4294967296),
            Just (FVString "1.10000"),
            Just (FVFloat 1.1),
            Just (FVDouble 1.1),
            Just (FVString "plátano"),
            Just (FVWString "香蕉"),
            Just (FVString "plátano"),
            Just (FVWString "香蕉"),
            Just (FVString "2013-12-31"),
            Just (FVString "23:34:56"),
            Just (FVString "2013-12-31 23:34:56")
           ]
          ]
  records <- readCsvRecords Nothing inputFilename
  assertEqual "Parse from CSV" expectedRecord records

  newCsv <- csv2yxdb2csv C2Y.defaultSettings inputFilename outputFilename
  yxdbFile <- decodeFile outputFilename :: IO YxdbFile

  T.writeFile secondOutputFilename newCsv
  newRecords <- readCsvRecords Nothing secondOutputFilename

  assertEqual "Parse from YXDB" expectedRecord (yxdbFile ^. yxdbFileRecords)
  assertEqual "New CSV doesn't match old" expectedRecord newRecords

test_signedRecordParsing :: Assertion
test_signedRecordParsing = do
  let inputFilename = "test-data/signed.csv"
      outputFilename = "test-data/signed.yxdb"
      secondOutputFilename = "test-data/signed.yxdb.csv"
      expectedRecord =
          [
           Record [
            Just (FVByte $ -1),
            Just (FVInt16 $ -256),
            Just (FVInt32 $ -32768),
            Just (FVInt64 $ -4294967296),
            Just (FVString "1.10000"),
            Just (FVFloat $ -1.1),
            Just (FVDouble $ -1.1)
           ]
          ]
  records <- readCsvRecords Nothing inputFilename
  assertEqual "Parse from CSV" expectedRecord records

  newCsv <- csv2yxdb2csv C2Y.defaultSettings inputFilename outputFilename
  yxdbFile <- decodeFile outputFilename :: IO YxdbFile

  T.writeFile secondOutputFilename newCsv
  newRecords <- readCsvRecords Nothing secondOutputFilename

  assertEqual "Parse from YXDB" expectedRecord (yxdbFile ^. yxdbFileRecords)
  assertEqual "New CSV doesn't match old" expectedRecord newRecords

test_multipleBlocks :: Assertion
test_multipleBlocks = do
  let inputFilename = "test-data/multiple-blocks.csv"
      outputFilename = "test-data/multiple-blocks.yxdb"
  oldCsv <- T.readFile inputFilename
  newCsv <- csv2yxdb2csv C2Y.defaultSettings inputFilename outputFilename
  assertDiffEqual oldCsv newCsv

yxdbTests :: Test.Framework.Test
yxdbTests =
    testGroup "YXDB" [
        testProperty "Header length" prop_HeaderLength,
        testProperty "Metadata length" prop_MetadataLength,
--        testProperty "Blocks length" prop_BlocksLength, --
        testProperty "Block Index get & put inverses" prop_BlockIndexGetAndPutAreInverses,
        testProperty "Metadata get and put inverses" prop_MetadataGetAndPutAreInverses,
        testProperty "Header get & put inverses" prop_HeaderGetAndPutAreInverses,
        testProperty "Value get & put inverses" prop_ValueGetAndPutAreInverses,
        testProperty "Blocks get & put inverses" prop_BlocksGetAndPutAreInverses,
--        testProperty "Yxdb get & put inverses" prop_YxdbFileGetAndPutAreInverses,
        testCase "Loading small module" test_LoadingSmallModule,
        testCase "CSV to YXDB to CSV is the identity" test_csv2yxdb2csvIsIdentity,
        testCase "Quotes are not used to escape" test_csv2yxdbQuoteParsing,
        testCase "Can parse ISO style dates" test_csv2yxdbDateFormat,
        testCase "Example CSV parses into correct structures" test_recordParsing,
        testCase "Example CSV with signed values" test_signedRecordParsing,
        testCase "Rendering a non-ANSI codepoint" test_renderNonAnsiCodepoint,
        testCase "Rendering a non-ANSI codepoint at record level" test_renderNonAnsiCodepointRecord,
        testCase "Rendering a non-ANSI codepoint from CSV to CSV" test_renderNonAnsiCodepointCSVRecord,
        testCase "Rendering a non-ANSI codepoint from CSV to YXDB blocks to CSV" test_renderNonAnsiCodepointCSV2Record2Block2Record2CSV,
        testCase "Multiple blocks" test_multipleBlocks
    ]
