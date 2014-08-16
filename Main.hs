import           Data.Binary
import           Data.Binary.Get
import           Database.Alteryx

exampleFile = "small-module.yxdb"

parsed :: IO (Either (ByteOffset, String) YxdbFile)
parsed = decodeFileOrFail exampleFile

main = do
  p <- parsed
  putStrLn $ show p
