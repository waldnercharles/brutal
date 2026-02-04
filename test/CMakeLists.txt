cmake_minimum_required(VERSION 4.2)

project(test LANGUAGES C)

file(
    GLOB_RECURSE PROJECT_SOURCES
    CONFIGURE_DEPENDS
    "${CMAKE_SOURCE_DIR}/src/*.c"
)

# set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/bin")

# Test
file(
    GLOB_RECURSE TEST_SOURCES
    CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/test_*.c"
)
add_executable(
    test
    "${CMAKE_CURRENT_SOURCE_DIR}/test.c"
    ${TEST_SOURCES}
    ${PROJECT_SOURCES}
)
target_compile_options(
    test
    PRIVATE -Wall -Wextra -Wpedantic -Werror -Wno-unused-function
)

target_include_directories(test PUBLIC "${CMAKE_SOURCE_DIR}/include")
set_target_properties(
    test
    PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/bin"
)

# Benchmark
add_executable(
    benchmark
    "${CMAKE_CURRENT_SOURCE_DIR}/benchmark.c"
    ${PROJECT_SOURCES}
)
target_compile_options(
    benchmark
    PRIVATE
        -O2
        -Wall
        -Wextra
        -Wpedantic
        -Wno-unused-function
        -Wno-strict-prototypes
)
target_include_directories(benchmark PUBLIC "${CMAKE_SOURCE_DIR}/include")

add_executable(
    benchmark_tpool_compare
    "${CMAKE_CURRENT_SOURCE_DIR}/bench_tpool_compare.c"
    ${PROJECT_SOURCES}
)
target_compile_options(
    benchmark_tpool_compare
    PRIVATE
        -O2
        -Wall
        -Wextra
        -Wpedantic
        -Werror
        -Wno-unused-function
        -Wno-strict-prototypes
)
target_include_directories(benchmark_tpool_compare PUBLIC "${CMAKE_SOURCE_DIR}/include")
