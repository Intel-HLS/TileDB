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

# Build as external project

include(ExternalProject)

ExternalProject_Add(
  HDFS
  DOWNLOAD_COMMAND ""
  SOURCE_DIR ${HDFS_SOURCE_DIR}
  STEP_TARGETS build
  EXCLUDE_FROM_ALL TRUE
  )

ExternalProject_Get_Property(HDFS BINARY_DIR)
set(HDFS_OBJECTS_DIR ${BINARY_DIR}/hdfs_objs)

find_path(HDFS_INCLUDE_DIR hdfs.h HINTS ${HDFS_SOURCE_DIR}/main/native/libhdfs)
list(APPEND HDFS_OBJS 
  ${HDFS_OBJECTS_DIR}/htable.c.o
  ${HDFS_OBJECTS_DIR}/mutexes.c.o
  ${HDFS_OBJECTS_DIR}/thread_local_storage.c.o
  ${HDFS_OBJECTS_DIR}/exception.c.o
  ${HDFS_OBJECTS_DIR}/jni_helper.c.o
  ${HDFS_OBJECTS_DIR}/hdfs.c.o
  )

find_package(JNI REQUIRED)

include(FindPackageHandleStandardArgs)


