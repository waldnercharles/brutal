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

test_arch: configure
	cmake --build build --config Debug --target arch_ecs_test_O2 -j
	@./bin/Debug/arch_ecs_test_O2

bench_arch: configure
	cmake --build build --config Debug --target arch_ecs_benchmark -j
	@./bin/Debug/arch_ecs_benchmark

run: test

