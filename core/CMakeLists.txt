#
# core/CMakeLists.txt
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

# Include core header directories
file(GLOB TILEDB_CORE_INCLUDE_DIRS "include/*")
include_directories(${TILEDB_CORE_INCLUDE_DIRS})

# Gather the core source files
file(GLOB_RECURSE TILEDB_CORE_SOURCES "src/*.cc")

# Compile sources with PIC
add_library(TILEDB_CORE_OBJECTS OBJECT ${TILEDB_CORE_SOURCES})
set_property(TARGET TILEDB_CORE_OBJECTS PROPERTY POSITION_INDEPENDENT_CODE ON)

# Create static library
add_library(tiledb_static STATIC $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS>)
set_target_properties(tiledb_static PROPERTIES OUTPUT_NAME "tiledb")

# Create dynamic library
add_library(tiledb_shared SHARED $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS>)
set_target_properties(tiledb_shared PROPERTIES OUTPUT_NAME "tiledb")
target_link_libraries(tiledb_shared ${TILEDB_LIB_DEPENDENCIES})

# Install libraries
install(
    TARGETS tiledb_static tiledb_shared 
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)
file(GLOB TILEDB_HEADERS "include/c_api/*.h")
install(FILES ${TILEDB_HEADERS} DESTINATION include)

