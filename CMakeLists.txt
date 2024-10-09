cmake_minimum_required(VERSION 3.21)

option(CXX "enable C++ compilation" ON)
if (CXX)
  enable_language(CXX)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR})

project(DSTree VERSION 1.0.0)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_definitions(-DDEBUG)
endif()

set(EXECUTABLE_NAME main)
set(SRC dstree)
set(TEST test)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

find_package(fmt REQUIRED)
find_package(PostgreSQL REQUIRED)
find_package(Boost REQUIRED)

include_directories(
  ${PROJECT_SOURCE_DIR}/${SRC}
)

set(MAIN_NAME main)

file(GLOB ALL_SRCS
  "${PROJECT_SOURCE_DIR}/${TEST}/${MAIN_NAME}.cpp"
  "${PROJECT_SOURCE_DIR}/${SRC}/*.h"
  "${PROJECT_SOURCE_DIR}/${SRC}/*.c"
  "${PROJECT_SOURCE_DIR}/${SRC}/*.hpp"
  "${PROJECT_SOURCE_DIR}/${SRC}/*.cpp"
)

add_executable(${EXECUTABLE_NAME} ${ALL_SRCS})
target_link_libraries(${EXECUTABLE_NAME} fmt::fmt)
target_link_libraries(${EXECUTABLE_NAME} PostgreSQL::PostgreSQL)
target_link_libraries(${EXECUTABLE_NAME} boost::boost)
