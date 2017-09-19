# Determine compiler flags for MuParserX
# Once done this will define
# MUPARSERX_FOUND - MuParserX found

#Build as external project
include(ExternalProject)
ExternalProject_Add(
    MuParserX
    DOWNLOAD_COMMAND ""
    SOURCE_DIR "${CMAKE_SOURCE_DIR}/deps/muparserx"
    UPDATE_COMMAND ""
    PATCH_COMMAND ""
    CMAKE_ARGS ""
    INSTALL_COMMAND ""
)

ExternalProject_Add_Step(
    MuParserX Make
    COMMAND ${CMAKE_MAKE_COMMAND}
#    DEPENDEES install
)

set(MuParserX_INCLUDE_DIRS "${CMAKE_SOURCE_DIR}/deps/muparserx/parser")
set(MuParserX_LIBRARIES "${CMAKE_SHARED_LIBRARY_PREFIX}muparserx${CMAKE_SHARED_LIBRARY_PREFIX}")
include_directories(${MuParserX_INCLUDE_DIRS})
