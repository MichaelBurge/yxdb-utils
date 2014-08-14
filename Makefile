all: build

init:
	cabal sandbox init
	cabal install --only-dependencies

build:
	cabal configure --enable-tests --enable-library-profiling
	cabal build
	cabal test
