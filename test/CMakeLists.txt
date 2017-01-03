#Google test
find_package(GTest REQUIRED)
include_directories("${GTEST_INCLUDE_DIRS}")

set(TileDB_test_sources
    ./src/array/array_schema_spec.cc
    ./src/c_api/c_api_spec.cc
    )

#Compile sources with PIC
add_library(TileDB_test_object_files OBJECT ${TileDB_test_sources})
set_property(TARGET TileDB_test_object_files PROPERTY POSITION_INDEPENDENT_CODE ON)

add_executable(tiledb_test $<TARGET_OBJECTS:TileDB_core_library_object_files>)
target_link_libraries(tiledb_test tiledb_static ${GTEST_MAIN_LIBRARIES} ${GTEST_LIBRARIES} ${OPENSSL_LIBRARIES} ${ZLIB_LIBRARIES})
add_test(tiledb_test tiledb_test)
