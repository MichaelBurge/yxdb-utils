all: build

init:
	cabal sandbox init
	cabal install --only-dependencies

build:
	cabal configure --enable-tests
	cabal build
	cabal test
