function(build_TileDB_example target)
    add_executable(${target} src/${target}.cc)
    target_link_libraries(${target} tiledb_static ${OPENSSL_LIBRARIES} ${ZLIB_LIBRARIES})
    install(TARGETS ${target} RUNTIME DESTINATION bin)
endfunction()

if(NOT DISABLE_MPI)
    foreach(example_binary 
            tiledb_array_aio_read_dense
            tiledb_array_aio_write_dense
            tiledb_array_consolidate
            tiledb_array_create_dense
            tiledb_array_create_sparse
            tiledb_array_iterator_dense
            tiledb_array_iterator_sparse
            tiledb_array_parallel_consolidate_dense
            tiledb_array_parallel_consolidate_sparse
            tiledb_array_parallel_read_dense_1
            tiledb_array_parallel_read_dense_2
            tiledb_array_parallel_read_mpi_io_dense
            tiledb_array_parallel_read_sparse_1
            tiledb_array_parallel_read_sparse_2
            tiledb_array_parallel_write_dense_1
            tiledb_array_parallel_write_dense_2
            tiledb_array_parallel_write_sparse_1
            tiledb_array_parallel_write_sparse_2
            tiledb_array_primitive
            tiledb_array_read_dense_1
            tiledb_array_read_dense_2
            tiledb_array_read_dense_3
            tiledb_array_read_sparse_1
            tiledb_array_read_sparse_2
            tiledb_array_update_dense_1
            tiledb_array_update_dense_2
            tiledb_array_update_sparse_1
            tiledb_array_update_sparse_2
            tiledb_array_write_dense_1
            tiledb_array_write_dense_2
            tiledb_array_write_sparse_1
            tiledb_array_write_sparse_2
            tiledb_array_write_sparse_3
            tiledb_array_write_sparse_4
            tiledb_clear_delete_move
            tiledb_config
            tiledb_ls
            tiledb_ls_workspaces
            tiledb_metadata_consolidate
            tiledb_metadata_create
            tiledb_metadata_iterator
            tiledb_metadata_primitive
            tiledb_metadata_read
            tiledb_metadata_update
            tiledb_metadata_write
            tiledb_workspace_group_create)
        build_TileDB_example(${example_binary})
    endforeach(example_binary)
endif()
