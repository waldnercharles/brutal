.PHONY: all configure build test run

all: configure build run

configure:
	cmake --preset default

build: configure
	cmake --build --preset default -j

clean:
	rm -rf build bin

test: configure build
	@./bin/Debug/test

benchmark: configure build
	@./bin/Release/benchmark

run: test

