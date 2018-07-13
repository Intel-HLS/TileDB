#
# cmake/Modules/FindHDFS.cmake
#
# The MIT License
#
# Copyright (c) 2018 Omics Data Automation, Inc. and Intel Corporation.
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
if(CMAKE_CROSSCOMPILING)
    set(JAVA_HOME ${TARGET_JAVA_HOME})
    set(ENV{JAVA_HOME} ${TARGET_JAVA_HOME})
endif()
find_package(JNI REQUIRED)
if(CMAKE_CROSSCOMPILING)
    unset(JAVA_HOME)
    unset(ENV{JAVA_HOME})
endif()

find_path(HDFS_INCLUDE_DIR hdfs.h HINTS ${HDFS_SOURCE_DIR}/main/native/libhdfs)
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(HDFS "Could not find HDFS headers ${DEFAULT_MSG}" HDFS_INCLUDE_DIR)

include_directories(${HDFS_INCLUDE_DIR})
add_definitions(-DUSE_HDFS)

add_subdirectory(${HDFS_SOURCE_DIR} EXCLUDE_FROM_ALL)
