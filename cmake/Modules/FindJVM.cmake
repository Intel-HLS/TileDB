#
# The MIT License
#
# Copyright (c) 2017 Omics Data Automation, Inc.
#           (c) 2017 UCLA All rights reserved
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
## Setup jvm variables for include/link

set(JAVA_HOME "$ENV{JAVA_HOME}")
if(NOT JAVA_HOME)
  message(FATAL_ERROR "Undefined JAVA_HOME environment variable")
  return()
endif()
if(NOT EXISTS ${JAVA_HOME})
  message(FATAL_ERROR "JAVA_HOME=" ${JAVA_HOME} " NOT found")
  return()
endif()
if(NOT IS_DIRECTORY ${JAVA_HOME})
  message(FATAL_ERROR "JAVA_HOME=" ${JAVA_HOME} " NOT a directory")
  return()
endif()

message(STATUS "JAVA_HOME=" ${JAVA_HOME})

if (NOT IS_DIRECTORY ${JAVA_HOME}/include)
  message(FATAL_ERROR "JAVA_HOME=" ${JAVA_HOME}
	" does NOT seem to contain an include folder")
  return()
set(JVM_INCLUDE_PATH ${JAVA_HOME}/include)

find_library(JVM_LIBRARY jvm PATHS ${JAVA_HOME}/jre/lib/amd64/server)
if(JVM_LIBRARY)
  message(STATUS "Found jvm library in " ${JVM_LIBRARY})
else()
  message(FATAL_ERROR "jvm library could not be found")
  return()
endif()
