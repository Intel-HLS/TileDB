#
# FindHDFS.cmake
#
#
# The MIT License
#
# Copyright (c) UCLA. License pursuant to original Intel MIT license.
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

# Get java and hdfs include directories and libraries for linking in hdfs support
# This module sets the following result variables
#  For include
#   JNI_INCLUDE_DIRS
#   HDFS_INCLUDE_PATH
#  For link
#   JAVA_JVM_LIBRARY
#   HDFS_STATIC_LIBRARY
#   HDFS_SHARED_LIBRARY

# Using system provided FindJNI.cmake
find_package(JNI)
if (NOT JNI_FOUND)
  message(FATAL_ERROR "JAVA installation not found")
  return()
endif()
message(STATUS "Found JVM: " ${JAVA_JVM_LIBRARY}) 

if (NOT HADOOP_HOME) 
  set(HADOOP_HOME "$ENV{HADOOP_HOME}")
endif()
if(NOT HADOOP_HOME)
  message(FATAL_ERROR "Undefined HADOOP_HOME. Set environment variable or invoke cmake with -DHADOOP_HOME set to root of hadoop installation")
  return()
endif()
if(NOT EXISTS ${HADOOP_HOME})
  message(FATAL_ERROR "HADOOP_HOME=" ${HADOOP_HOME} " NOT found")
  return()
endif()
if (NOT IS_DIRECTORY ${HADOOP_HOME})
  message(FATAL_ERROR "HADOOP_HOME=" ${HADOOP_HOME} " NOT a directory")
  return()
endif()

message(STATUS "Found env HADOOP_HOME: " ${HADOOP_HOME})

if (IS_DIRECTORY ${HADOOP_HOME}/include)
  set(HDFS_INCLUDE_PATH ${JNI_INCLUDE_DIRS} ${HADOOP_HOME}/include)
else()
  message(FATAL_ERROR "HADOOP_HOME=" ${HADOOP_HOME}
	" does NOT seem to contain an include folder")
  return()
endif()

find_library(HDFS_STATIC_LIBRARY libhdfs.a PATHS ${HADOOP_HOME}/lib/native)
if(NOT HDFS_STATIC_LIBRARY)
  message(FATAL_ERROR "hdfs static library could not be found")
  return()
endif()

find_library(HDFS_SHARED_LIBRARY libhdfs.so PATHS ${HADOOP_HOME}/lib/native)
if(NOT HDFS_SHARED_LIBRARY)
  message(FATAL_ERROR "hdfs shared library could not be found")
  return()
endif()

message(STATUS "Found hdfs: " ${HDFS_STATIC_LIBRARY} " " ${HDFS_SHARED_LIBRARY})
