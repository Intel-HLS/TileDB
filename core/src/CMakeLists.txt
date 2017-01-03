set(TileDB_core_library_sources
    ./array/array.cc
    ./array/array_iterator.cc
    ./array/array_read_state.cc
    ./array/array_schema.cc
    ./c_api/c_api.cc
    ./fragment/book_keeping.cc
    ./fragment/fragment.cc
    ./fragment/read_state.cc
    ./fragment/write_state.cc
    ./metadata/metadata.cc
    ./metadata/metadata_iterator.cc
    ./misc/hilbert_curve.cc
    ./misc/utils.cc
    ./storage_manager/config.cc
    ./storage_manager/storage_manager.cc
    )

#Compile sources with PIC
add_library(TileDB_core_library_object_files OBJECT ${TileDB_core_library_sources})
set_property(TARGET TileDB_core_library_object_files PROPERTY POSITION_INDEPENDENT_CODE ON)

#Create the shared and static libraries
add_library(tiledb_static STATIC $<TARGET_OBJECTS:TileDB_core_library_object_files>)
#Link in other libraries for shared library
add_library(tiledb_shared SHARED $<TARGET_OBJECTS:TileDB_core_library_object_files>)
target_link_libraries(tiledb_shared ${OPENSSL_LIBRARIES} ${ZLIB_LIBRARIES})

install(TARGETS tiledb_static tiledb_shared LIBRARY DESTINATION lib ARCHIVE DESTINATION lib)
