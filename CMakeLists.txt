cmake_minimum_required(VERSION 4.2)

project(nmd LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_OBJECT_PATH_MAX 1024)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON CACHE STRING "")

add_compile_definitions(_DEFAULT_SOURCE)

set(CMAKE_C_FLAGS_DEBUG
    "-g3 -O0 -ggdb"
    CACHE STRING
    "Flags used by the C compiler during debug builds."
    FORCE
)
set(CMAKE_C_FLAGS_RELEASE
    "-O2 -DNDEBUG"
    CACHE STRING
    "Flags used by the C compiler during release builds."
    FORCE
)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/bin")

link_directories("${CMAKE_BINARY_DIR}/lib")

add_subdirectory(test)

file(GLOB_RECURSE SOURCES CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/src/*.c")
file(GLOB_RECURSE HEADERS CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/include/*.h")

file(
    CREATE_LINK
        "${CMAKE_BINARY_DIR}/compile_commands.json"
        "${CMAKE_SOURCE_DIR}/compile_commands.json"
    SYMBOLIC
)
