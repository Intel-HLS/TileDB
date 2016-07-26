project(TileDB)
cmake_minimum_required(VERSION 2.8)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

#Build parameters
set(DISABLE_MPI False CACHE BOOL "Disable use of any MPI compiler/libraries")
set(DISABLE_OPENMP False CACHE BOOL "Disable OpenMP")
set(BUILD_DISTRIBUTABLE_LIBRARY False CACHE BOOL "Build the TileDB/GenomicsDB library with minimal runtime dependencies")
set(OPENSSL_PREFIX_DIR "" CACHE PATH "OpenSSL installation prefix")
set(BUILD_UNIT_TESTS False CACHE BOOL "Build unit tests - requires Google C++ testing framework: GTest")

if(BUILD_DISTRIBUTABLE_LIBRARY)
    cmake_minimum_required(VERSION 3.4)
    set(DISABLE_MPI True)
    set(DISABLE_OPENMP True)
    #For the GNU compiler, link in static gcc libraries
    if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -static-libgcc -static-libstdc++")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -static-libstdc++")
    endif()
    set(CMAKE_OSX_DEPLOYMENT_TARGET "10.9")
endif()

#Check C++ 2011 support
include(CheckAndSetStdCXX2011Flag)
CHECK_AND_SET_STD_CXX_2011_FLAG(test_cpp_2011)
if(NOT test_cpp_2011)
    message(FATAL_ERROR "Your compiler does not support C++ 2011, exiting")
endif()

#MPI library/compiler etc
if(NOT DISABLE_MPI)
    find_package(MPI REQUIRED)
    set(CMAKE_CXX_COMPILER ${MPI_CXX_COMPILER})
endif()

if(NOT DISABLE_OPENMP)
    find_package(OpenMP)
    if(NOT OPENMP_FOUND)
        set(DISABLE_OPENMP True)
    else()
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS} -DOPENMP")
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS} -DOPENMP")
    endif()
endif()

add_definitions(-D_FILE_OFFSET_BITS=64)  #large file support

#Produce verbose output
if(VERBOSE)
    add_definitions(-DVERBOSE=${VERBOSE})
    message(STATUS "Programs will produce output with verbosity=${VERBOSE}")
endif()

set(CMAKE_CXX_FLAGS_DEBUG "-DDEBUG -g3 -gdwarf-3 -Wall")
set(CMAKE_CXX_FLAGS_RELEASE "-DNDEBUG -O3 -fvisibility=hidden")
set(CMAKE_CXX_FLAGS_COVERAGE "-DDEBUG -g3 -gdwarf-3 --coverage")

#OpenSSL 
set(OPENSSL_ROOT_DIR "${OPENSSL_PREFIX_DIR}")
if(BUILD_DISTRIBUTABLE_LIBRARY)
    set(OPENSSL_USE_STATIC_LIBS True)
endif()
find_package(OpenSSL REQUIRED)
include_directories(OPENSSL_INCLUDE_DIR)

#Zlib
find_package(ZLIB REQUIRED)

include_directories (
    core/include/array
    core/include/c_api
    core/include/fragment
    core/include/metadata
    core/include/misc
    core/include/storage_manager
    )

add_subdirectory(core)
add_subdirectory(examples)

#Unit tests
if(BUILD_UNIT_TESTS)
    enable_testing()
    add_subdirectory(test)
endif()
