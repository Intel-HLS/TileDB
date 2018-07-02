#
# CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2016 MIT and Intel Corporation
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#

project(TileDB)
cmake_minimum_required(VERSION 2.8)

set(ENABLE_LZ4 False CACHE BOOL "Enable lz4 compression")
set(ENABLE_BLOSC False CACHE BOOL "Enable lz4 compression")
set(ENABLE_ZSTD False CACHE BOOL "Enable lz4 compression")
set(ENABLE_MUPARSERX_EXPRESSIONS False CACHE BOOL "Enable MuparserX expression parsing")
set(LIBUUID_DIR "" CACHE PATH "Path to libuuid install directory")
set(ENABLE_MASTER_CATALOG True CACHE BOOL "Enable master catalog")

# Set the cmake Module path
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

# Set shared library suffix
if(APPLE)
  set(SHARED_LIB_SUFFIX ".dylib")
else()
  set(SHARED_LIB_SUFFIX ".so")
endif()

# Only for Mac OS X warnings
if(APPLE)
  set(CMAKE_MACOSX_RPATH 1)
  set(CMAKE_FIND_FRAMEWORK NEVER)
endif()

# Default user definitions
set(USE_MPI False CACHE BOOL "Enables MPI")
if(APPLE)
  set(USE_OPENMP False CACHE BOOL "Enables OpenMP")
else()
  set(USE_OPENMP True CACHE BOOL "Enables OpenMP")
endif()
set(TILEDB_VERBOSE False CACHE BOOL "Prints TileDB errors with verbosity")
set(TILEDB_TRACE False CACHE BOOL "Trace TileDB C API")
set(MAC_ADDRESS_INTERFACE "" 
    CACHE STRING "The interface carrying the MAC address of the machine."
)
set(USE_PARALLEL_SORT False CACHE BOOL "Enables parallel sorting.")
set(GTEST_FILTER "*" CACHE STRING "Filters for the Google unit tests.")
set(COMPRESSION_LEVEL_GZIP "" CACHE STRING "Compression level for GZIP.")
set(COMPRESSION_LEVEL_ZSTD "" CACHE STRING "Compression level for Zstandard.")
set(COMPRESSION_LEVEL_BLOSC "" CACHE STRING "Compression level for Blosc.")
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

if(ENABLE_MASTER_CATALOG)
    add_definitions(-DENABLE_MASTER_CATALOG)
endif()

# Find required library dependencies
find_package(ZLIB REQUIRED)
include_directories(${ZLIB_INCLUDE_DIR})

find_package(libuuid REQUIRED)
include_directories(${LIBUUID_INCLUDE_DIR})

set(TILEDB_LIB_DEPENDENCIES ${ZLIB_LIBRARIES} ${LIBUUID_LIBRARY})
if(ENABLE_LZ4)
    find_package(LZ4 REQUIRED)
    include_directories(${LZ4_INCLUDE_DIR})
    set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${LZ4_LIBRARIES})
    add_definitions(-DENABLE_LZ4)
endif()
if(ENABLE_BLOSC)
    find_package(BLOSC REQUIRED)
    include_directories(${BLOSC_INCLUDE_DIR})
    set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${BLOSC_LIBRARIES})
    add_definitions(-DENABLE_BLOSC)
endif()
if(ENABLE_ZSTD)
    find_package(ZSTD REQUIRED)
    include_directories(${ZSTD_INCLUDE_DIR})
    set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${ZSTD_LIBRARIES})
    add_definitions(-DENABLE_ZSTD)
endif()
find_package(OpenSSL REQUIRED)
include_directories(${OPENSSL_INCLUDE_DIR})
set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${OPENSSL_LIBRARIES})

# Find optional library dependencies
find_package(GTest)
find_package(Doxygen)
if(USE_MPI)
  find_package(MPI REQUIRED)
  include_directories(${MPI_CXX_INCLUDE_PATH})
  set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${MPI_CXX_LIBRARIES})
endif()
if(USE_OPENMP AND NOT APPLE)
  find_package(OpenMP REQUIRED)
endif()

# Add pthread library to dependencies
set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} pthread)

# Add HDFS support
set(HDFS_SOURCE_DIR "${CMAKE_SOURCE_DIR}/deps/HDFSWrapper/hadoop-hdfs-native")
find_package(HDFS REQUIRED)
include_directories(${HDFS_INCLUDE_DIR})
set_source_files_properties(${HDFS_OBJS} PROPERTIES GENERATED TRUE)
set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${JAVA_JVM_LIBRARY} ${CMAKE_DL_LIBS})

# Set C++ 2011 flag
include(SetCXX2011Flag)

if(ENABLE_MUPARSERX_EXPRESSIONS)
    # Build MuParserX source code added as a dependency
    include(MuParserX)
    add_definitions(-DENABLE_MUPARSERX_EXPRESSIONS)
endif()

get_property(dirs DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)

# Set compiler flags
set(CMAKE_CXX_FLAGS_DEBUG "-DDEBUG -g3 -gdwarf-3 -Wall -fvisibility=hidden")
set(CMAKE_CXX_FLAGS_RELEASE "-DNDEBUG -O3 -fvisibility=hidden")
set(CMAKE_CXX_FLAGS_COVERAGE "-DDEBUG -g3 -gdwarf-3 --coverage")
if(NOT APPLE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-literal-suffix")
endif()
if(USE_MPI)
  set(CMAKE_CXX_COMPILER ${MPI_CXX_COMPILER})
endif()
if(USE_OPENMP AND NOT APPLE)
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
endif()

# Add definitions
add_definitions(-D_FILE_OFFSET_BITS=64)
if(USE_MPI)
  add_definitions(-DHAVE_MPI)
endif()
if(USE_OPENMP AND NOT APPLE)
  add_definitions(-DHAVE_OPENMP)
endif()
if(TILEDB_VERBOSE)
  add_definitions(-DTILEDB_VERBOSE)
  message(STATUS "The TileDB library is compiled with verbosity.")
endif()
if(TILEDB_TRACE)
  if(CMAKE_BUILD_TYPE STREQUAL "Debug")
     add_definitions(-DTILEDB_TRACE)
     message(STATUS "The TileDB library is compiled with trace.")
  else()
     message(STATUS "TileDB in release mode cannot have trace. CMAKE_BUILD_TYPE=" ${CMAKE_BUILD_TYPE})
  endif()
endif()
if(MAC_ADDRESS_INTERFACE)
  add_definitions(-DTILEDB_MAC_ADDRESS_INTERFACE=${MAC_ADDRESS_INTERFACE})
  message(STATUS "Set MAC address interface to ${MAC_ADDRESS_INTERFACE}.")
endif()
if(USE_PARALLEL_SORT)
  add_definitions(-DUSE_PARALLEL_SORT)
  message(STATUS "Will use parallel sort.")
endif()
if(COMPRESSION_LEVEL_GZIP)
  add_definitions(-DTILEDB_COMPRESSION_LEVEL_GZIP=${COMPRESSION_LEVEL_GZIP})
  message(STATUS "Set GZIP compression level to ${COMPRESSION_LEVEL_GZIP}.")
endif()
if(COMPRESSION_LEVEL_ZSTD)
  add_definitions(-DTILEDB_COMPRESSION_LEVEL_ZSTD=${COMPRESSION_LEVEL_ZSTD})
  message(STATUS 
          "Set Zstandard compression level to ${COMPRESSION_LEVEL_ZSTD}."
)
endif()
if(COMPRESSION_LEVEL_BLOSC)
  add_definitions(-DTILEDB_COMPRESSION_LEVEL_BLOSC=${COMPRESSION_LEVEL_BLOSC})
  message(STATUS "Set Blosc compression level to ${COMPRESSION_LEVEL_BLOSC}.")
endif()

# Enable testing
enable_testing()

# Build TileDB library
add_subdirectory(core)

# Build examples
add_subdirectory(examples)

# Build unit tests
if(GTEST_FOUND)
  add_subdirectory(test)
endif()

# Doxygen documentation
if(DOXYGEN_FOUND)
  file(GLOB_RECURSE TILEDB_CORE_HEADERS "core/include/*.h")
  add_custom_command(
     OUTPUT ${CMAKE_BINARY_DIR}/doxyfile.in
      COMMAND mkdir -p doxygen
      COMMAND echo INPUT = ${CMAKE_SOURCE_DIR}/doc/mainpage.dox 
              ${TILEDB_CORE_HEADERS} > ${CMAKE_BINARY_DIR}/doxyfile.in
      COMMAND echo FILE_PATTERNS = *.h >> ${CMAKE_BINARY_DIR}/doxyfile.in
      COMMENT "Preparing for Doxygen documentation" VERBATIM
  )
  add_custom_target(
      doc ${DOXYGEN_EXECUTABLE} ${CMAKE_SOURCE_DIR}/doc/Doxyfile.mk > 
          ${CMAKE_BINARY_DIR}/Doxyfile.log 2>&1
      COMMENT "Generating API documentation with Doxygen" VERBATIM
      DEPENDS ${CMAKE_BINARY_DIR}/doxyfile.in
  )
endif(DOXYGEN_FOUND)

# Uninstall
set(CMD "xargs printf '-- Uninstalling: %s\\\\n' <install_manifest.txt") 
add_custom_target(
   uninstall 
   COMMAND echo "Uninstalling TileDB..."
   COMMAND eval "${CMD}"
   COMMAND xargs rm -f < install_manifest.txt
   COMMAND echo "TileDB uninstalled"
)
