all: build

build:
	docker build --tag=yxdb-utils .

build_test:
	cabal sandbox init
	cabal install --only-dependencies --enable-tests -j4
	cabal configure --enable-tests --enable-library-profiling
	cabal build
	cabal test
