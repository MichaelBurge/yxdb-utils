import Test.Framework (defaultMain)

import Tests.Data.Binary.C (cBinaryTests)
import Tests.Codec.Compression.LZF.ByteString (lzfByteStringTests)
--import Tests.Database.Alteryx (yxdbTests)

main = defaultMain tests
tests =
    [
     cBinaryTests,
     lzfByteStringTests
--     yxdbTests
    ]
