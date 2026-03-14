cmake_minimum_required(VERSION 4.2)

project(test LANGUAGES C)
set(CMAKE_C_STANDARD 23)
set(CMAKE_C_STANDARD_REQUIRED ON)

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
list(FILTER TEST_SOURCES EXCLUDE REGEX "test_fat_ecs2\\.c$")
add_executable(
    test
    "${CMAKE_CURRENT_SOURCE_DIR}/test.c"
    ${TEST_SOURCES}
    ${PROJECT_SOURCES}
)
target_compile_options(
    test
    PRIVATE
        -std=c23
        -Wall
        -Wextra
        -Wpedantic
        -Werror
        -Wno-unused-function
        -Wno-strict-prototypes
)

target_compile_definitions(test PRIVATE ECS_CACHE_LINE=128 TPOOL_CACHE_LINE=128)

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
target_compile_definitions(
    benchmark
    PRIVATE ECS_CACHE_LINE=128 TPOOL_CACHE_LINE=128
)
target_include_directories(benchmark PUBLIC "${CMAKE_SOURCE_DIR}/include")

# Arch ECS test at -O2
add_executable(
    arch_ecs_test_O2
    "${CMAKE_CURRENT_SOURCE_DIR}/runner_arch_ecs.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/test_arch_ecs.c"
    "${CMAKE_SOURCE_DIR}/src/brutal_tpool.c"
)
target_compile_options(
    arch_ecs_test_O2
    PRIVATE
        -std=c23
        -O2
        -Wall
        -Wextra
        -Wpedantic
        -Werror
        -Wno-unused-function
        -Wno-strict-prototypes
)
target_include_directories(
    arch_ecs_test_O2
    PUBLIC "${CMAKE_SOURCE_DIR}/include" "${CMAKE_CURRENT_SOURCE_DIR}"
)

# Arch ECS benchmark
add_executable(
    arch_ecs_benchmark
    "${CMAKE_CURRENT_SOURCE_DIR}/benchmark_arch_ecs.c"
    "${CMAKE_SOURCE_DIR}/src/brutal_tpool.c"
)
target_compile_options(
    arch_ecs_benchmark
    PRIVATE
        -std=c23
        -O2
        -Wall
        -Wextra
        -Wpedantic
        -Wno-unused-function
        -Wno-strict-prototypes
)
target_include_directories(
    arch_ecs_benchmark
    PUBLIC "${CMAKE_SOURCE_DIR}/include"
)
