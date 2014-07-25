import Test.Framework (defaultMain)

import Tests.Database.Alteryx (yxdbTests)

main = defaultMain tests
tests = [
    yxdbTests
]
