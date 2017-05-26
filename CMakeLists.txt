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
endif()

# Default user definitions
set(USE_MPI False CACHE BOOL "Enables MPI")
set(USE_BLOSC_COMPRESSION False CACHE BOOL "Enables BLOSC Compression")
set(USE_ZSTD_COMPRESSION False CACHE BOOL "Enables Facebook Zstandard compression")
set(USE_LZ4_COMPRESSION False CACHE BOOL "Enables LZ4 compression")
if(APPLE)
  set(USE_OPENMP False CACHE BOOL "Enables OpenMP")
else()
  set(USE_OPENMP True CACHE BOOL "Enables OpenMP")
endif()
set(TILEDB_VERBOSE False CACHE BOOL "Prints TileDB errors with verbosity")
set(MAC_ADDRESS_INTERFACE "" 
    CACHE STRING "The interface carrying the MAC address of the machine."
)
set(USE_PARALLEL_SORT False CACHE BOOL "Enables parallel sorting.")
set(GTEST_FILTER "*" CACHE STRING "Filters for the Google unit tests.")
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Find required library dependencies
find_package(ZLIB REQUIRED)
include_directories(${ZLIB_INCLUDE_DIR})
set(TILEDB_LIB_DEPENDENCIES ${ZLIB_LIBRARIES})
if(USE_LZ4_COMPRESSION)
  find_package(LZ4 REQUIRED)
  include_directories(${LZ4_INCLUDE_DIR})
  set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${LZ4_LIBRARIES})
endif()
if(USE_BLOSC_COMPRESSION)
  find_package(BLOSC REQUIRED)
  include_directories(${BLOSC_INCLUDE_DIR})
  set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${BLOSC_LIBRARIES})
endif()
if(USE_ZSTD_COMPRESSION)
  find_package(ZSTD REQUIRED)
  include_directories(${ZSTD_INCLUDE_DIR})
  set(TILEDB_LIB_DEPENDENCIES ${TILEDB_LIB_DEPENDENCIES} ${ZSTD_LIBRARIES})
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


# Set C++ 2011 flag
include(SetCXX2011Flag)

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
if(MAC_ADDRESS_INTERFACE)
  add_definitions(-DTILEDB_MAC_ADDRESS_INTERFACE=${MAC_ADDRESS_INTERFACE})
  message(STATUS "Set MAC address interface to ${MAC_ADDRESS_INTERFACE}.")
endif()
if(USE_PARALLEL_SORT)
  add_definitions(-DUSE_PARALLEL_SORT)
  message(STATUS "Will use parallel sort.")
endif()
if(USE_ZSTD_COMPRESSION)
  add_definitions(-DHAVE_ZSTD)
  message(STATUS "TileDB library is compiled with Zstandard compression library.")
endif()
if(USE_BLOSC_COMPRESSION)
  add_definitions(-DHAVE_BLOSC)
  message(STATUS "TileDB library is compiled with Blosc compression library.")
endif()
if(USE_LZ4_COMPRESSION)
  add_definitions(-DHAVE_LZ4)
  message(STATUS "TileDB library is compiled with Lz4 compression library.")
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
