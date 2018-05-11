/**
 * @file   utils.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * 
 * @section DESCRIPTION
 *
 * This file contains useful (global) functions.
 */

#ifndef __UTILS_H__
#define __UTILS_H__

#include "storage_fs.h"

#ifdef HAVE_MPI
  #include <mpi.h>
#endif
#include <pthread.h>
#include <string>
#include <vector>


#ifdef HAVE_OPENMP
  #include <omp.h>
#endif

#include "tiledb_constants.h"

#ifdef TILEDB_TRACE
#  define TRACE_FN std::cerr << "Trace - Function:" << __func__ << " File:" << __FILE__ << ":" << __LINE__ << " tid=" << syscall(SYS_gettid) << std::endl << std::flush
#  define TRACE_FN_ARG(X) std::cerr << "Trace - Function:" << __func__ <<  " File:" << __FILE__ << ":" << __LINE__ << " " << X << " tid=" << syscall(SYS_gettid) << std::endl << st
d::flush
#else
#  define TRACE_FN
#  define TRACE_FN_ARG(X)
#endif

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**@{*/
/** Return code. */
#define TILEDB_UT_OK         0
#define TILEDB_UT_ERR       -1
/**@}*/

/** Default error message. */
#define TILEDB_UT_ERRMSG std::string("[TileDB::utils] Error: ")

/** Maximum number of bytes written in a single I/O. */
#define TILEDB_UT_MAX_WRITE_COUNT 1500000000    // ~ 1.5 GB


/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

/** Stores potential error messages. */
extern std::string tiledb_ut_errmsg;


/* ********************************* */
/*             FUNCTIONS             */
/* ********************************* */

/** Returns true if the input is an array read mode. */
bool array_read_mode(int mode); 

/** Returns true if the input is an array write mode. */
bool array_write_mode(int mode); 

/** Returns true if the input is an array filter mode. */
bool array_filter_mode(int mode);

/**
 * Checks if both inputs represent the '/' character. This is an auxiliary
 * function to adjacent_slashes_dedup().
 */
bool both_slashes(char a, char b);

/** 
 * Checks if the input cell is inside the input subarray. 
 *
 * @tparam T The type of the cell and subarray.
 * @param cell The cell to be checked.
 * @param subarray The subarray to be checked, expresses as [low, high] pairs
 *     along each dimension.
 * @param dim_num The number of dimensions for the cell and subarray.
 * @return *true* if the input cell is inside the input range and
 *     *false* otherwise.
 */
template<class T>
bool cell_in_subarray(const T* cell, const T* subarray, int dim_num);

/** 
 * Returns the number of cells in the input subarray (considering that the
 * subarray is dense). 
 *
 * @tparam T The type of the subarray.
 * @param subarray The input subarray.
 * @param dim_num The number of dimensions of the subarray.
 * @return The number of cells in the input subarray.
 */
template<class T>
int64_t cell_num_in_subarray(const T* subarray, int dim_num);

/**
 * Compares the precedence of two coordinates based on the column-major order.
 *
 * @tparam T The type of the input coordinates.
 * @param coords_a The first coordinates.
 * @param coords_b The second coordinates.
 * @param dim_num The number of dimensions of the coordinates.
 * @return -1 if *coords_a* precedes *coords_b*, 0 if *coords_a* and
 *     *coords_b* are equal, and +1 if *coords_a* succeeds *coords_b*.
 */
template<class T>
int cmp_col_order(
    const T* coords_a, 
    const T* coords_b, 
    int dim_num); 

/**
 * Compares the precedence of two coordinates associated with ids,
 * first on their ids (the smaller preceeds the larger) and then based 
 * on the column-major order.
 *
 * @tparam T The type of the input coordinates.
 * @param id_a The id of the first coordinates.
 * @param coords_a The first coordinates.
 * @param id_b The id of the second coordinates.
 * @param coords_b The second coordinates.
 * @param dim_num The number of dimensions of the coordinates.
 * @return -1 if *coords_a* precedes *coords_b*, 0 if *coords_a* and
 *     *coords_b* are equal, and +1 if *coords_a* succeeds *coords_b*.
 */
template<class T>
int cmp_col_order(
    int64_t id_a,
    const T* coords_a, 
    int64_t id_b,
    const T* coords_b, 
    int dim_num);

/**
 * Compares the precedence of two coordinates based on the row-major order.
 *
 * @tparam T The type of the input coordinates.
 * @param coords_a The first coordinates.
 * @param coords_b The second coordinates.
 * @param dim_num The number of dimensions of the coordinates.
 * @return -1 if *coords_a* precedes *coords_b*, 0 if *coords_a* and
 *     *coords_b* are equal, and +1 if *coords_a* succeeds *coords_b*.
 */
template<class T>
int cmp_row_order(
    const T* coords_a, 
    const T* coords_b, 
    int dim_num); 

/**
 * Compares the precedence of two coordinates associated with ids,
 * first on their ids (the smaller preceeds the larger) and then based 
 * on the row-major order.
 *
 * @tparam T The type of the input coordinates.
 * @param id_a The id of the first coordinates.
 * @param coords_a The first coordinates.
 * @param id_b The id of the second coordinates.
 * @param coords_b The second coordinates.
 * @param dim_num The number of dimensions of the coordinates.
 * @return -1 if *coords_a* precedes *coords_b*, 0 if *coords_a* and
 *     *coords_b* are equal, and +1 if *coords_a* succeeds *coords_b*.
 */
template<class T>
int cmp_row_order(
    int64_t id_a, 
    const T* coords_a, 
    int64_t id_b, 
    const T* coords_b, 
    int dim_num);

/**
 * Checks if a given pathURL is GCS.
 * @param pathURL URL to path to be checked.
 * @return true if pathURL starts with gs://
 */
bool is_gcs_path(const std::string& pathURL);

/**
 * Checks if a given pathURL is HDFS.
 * @param pathURL URL to path to be checked.
 * @return true of pathURL is HDFS compliant.
 */
bool is_hdfs_path(const std::string& pathURL);

/**
 * Creates a new directory.
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param dir The name of the directory to be created.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error. 
 */
int create_dir(StorageFS *fs, const std::string& dir);

/**
 * Creates a new file with the given flags and mode.
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param filename The name of the file to be created.
 * @param flags Status and Access mode flags for the file.
 * @param mode Permissions for the file to be created
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error. 
 */
int create_file(StorageFS *fs, const std::string& filename, int flags, mode_t mode);

/**
 * Deletes a file from the filesystem
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param filename The name of the file to be deleted.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error. 
 */
int delete_file(StorageFS *fs, const std::string& filename);

/**
 * Creates a special file to indicate that the input directory is a
 * TileDB fragment.
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param dir The name of the fragment directory where the file is created.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error. 
 */
int create_fragment_file(StorageFS *fs, const std::string& dir);

/** 
 * Returns the directory where the program is executed. 
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @return The directory where the program is executed. If the program cannot
 *     retrieve the current working directory, the empty string is returned.
 */
std::string current_dir(StorageFS *fs);

/**
 * Deletes a directory. Note that the directory must not contain other
 * directories, but it should only contain files.
 *
 * @param dirname The name of the directory to be deleted.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error. 
 */
int delete_dir(StorageFS *fs, const std::string& dirname);

/**
 * Checks if the input is a special TileDB empty value.
 *
 * @tparam T The type of the input value.
 * @param value The value to be checked.
 * @return *true* if the input value is a special TileDB empty value, and 
 *     *false* otherwise.
 */
template<class T>
bool empty_value(T value);

/** 
 * Doubles the size of the buffer.
 *
 * @param buffer The buffer to be expanded. 
 * @param buffer_allocated_size The original allocated size of the buffer.
 *     After the function call, this size doubles.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
int expand_buffer(void*& buffer, size_t& buffer_allocated_size);

/**
 * Expands the input MBR so that it encompasses the input coordinates.
 *
 * @tparam T The type of the MBR and coordinates.
 * @param mbr The input MBR to be expanded.
 * @param coords The input coordinates to expand the MBR.
 * @param dim_num The number of dimensions of the MBR and coordinates.
 * @return void
 */
template<class T>
void expand_mbr(T* mbr, const T* coords, int dim_num);

/** 
 * Returns the size of the input file.
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param filename The name of the file whose size is to be retrieved.
 * @return The file size on success, and TILEDB_UT_ERR for error.
 */
size_t file_size(StorageFS *fs, const std::string& filename);

/** Returns the names of the directories inside the input directory.
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param dir The input directory.
 * @return The vector of directories contained in the input directory.
 */  
std::vector<std::string> get_dirs(StorageFS *fs, const std::string& dir);

/** Returns the names of the files inside the input directory.
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param dir The input directory.
 * @return The vector of directories contained in the input directory.
 */
std::vector<std::string> get_files(StorageFS *fs, const std::string& dir);

/** Returns the names of the fragments inside the input directory.
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param dir The input directory.
 * @return The vector of directories that are associated with fragments contained
 *         in the input directory.
 */
std::vector<std::string> get_fragment_dirs(StorageFS *fs, const std::string& dir);

/** 
 * Returns the MAC address of the machine as a 12-char string, e.g.,
 * 00332a0b8c64. Returns an empty string upon error.
 */
std::string get_mac_addr();

/** 
 * GZIPs the input buffer and stores the result in the output buffer, returning
 * the size of compressed data. 
 *
 * @param in The input buffer.
 * @param in_size The size of the input buffer.
 * @param out The output buffer.
 * @param out_size The available size in the output buffer.
 * @return The size of compressed data on success, and TILEDB_UT_ERR on error.
 */
ssize_t gzip(
    unsigned char* in, 
    size_t in_size, 
    unsigned char* out, 
    size_t out_size,
    const int level);

/** 
 * Decompresses the GZIPed input buffer and stores the result in the output 
 * buffer, of maximum size avail_out. 
 *
 * @param in The input buffer.
 * @param in_size The size of the input buffer.
 * @param out The output buffer.
 * @param avail_out_size The available size in the output buffer.
 * @param out_size The size of the decompressed data.
 * @return TILEDB_UT_OK on success and TILEDB_UT_ERR on error.
 */
int gunzip(
    unsigned char* in, 
    size_t in_size, 
    unsigned char* out, 
    size_t avail_out_size, 
    size_t& out_size);

/** 
 * Checks if there are duplicates in the input vector. 
 * 
 * @tparam T The type of the values in the input vector.
 * @param v The input vector.
 * @return *true* if the vector has duplicates, and *false* otherwise.
 */
template<class T>
bool has_duplicates(const std::vector<T>& v);

/**
 * Checks if the input coordinates lie inside the input subarray.
 *
 * @tparam T The coordinates and subarray type.
 * @param coords The input coordinates.
 * @param subarray The input subarray.
 * @param dim_num The number of dimensions of the subarray.
 * @return *true* if the coordinates lie in the subarray, and *false* otherwise.
 */
template<class T>
bool inside_subarray(const T* coords, const T* subarray, int dim_num);

/** 
 * Checks if the input vectors have common elements. 
 *
 * @tparam T The type of the elements of the input vectors.
 * @param v1 The first input vector.
 * @param v2 The second input vector.
 * @return *true* if the input vectors have common elements, and *false*
 *     otherwise.
 */
template<class T>
bool intersect(const std::vector<T>& v1, const std::vector<T>& v2);

/**
 * Checks if the input directory is an array.
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param dir The directory to be checked.
 * @return *true* if the directory is an array, and *false* otherwise.
 */
bool is_array(StorageFS *fs, const std::string& dir);

/**
 * Checks if one range is fully contained in another.
 *
 * @tparam The domain type
 * @param range_A The first range.
 * @param range_B The second range.
 * @param dim_num The number of dimensions.
 * @return True if range_A is fully contained in range_B. 
 */
template<class T>
bool is_contained(
    const T* range_A, 
    const T* range_B, 
    int dim_num);

/** 
 * Checks if the input is an existing directory. 
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param dir The directory to be checked.
 * @return *true* if *dir* is an existing directory, and *false* otherwise.
 */ 
bool is_dir(StorageFS *fs, const std::string& dir);

/** 
 * Checks if the input is an existing file. 
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param file The file to be checked.
 * @return tTrue* if *file* is an existing file, and *false* otherwise.
 */ 
bool is_file(StorageFS *fs, const std::string& file);

/**
 * Checks if the input directory is a fragment.
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param dir The directory to be checked.
 * @return *true* if the directory is a fragment, and *false* otherwise.
 */
bool is_fragment(StorageFS *fs, const std::string& dir);

/**
 * Checks if the input directory is a group.
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param dir The directory to be checked.
 * @return *true* if the directory is a group, and *false* otherwise.
 */
bool is_group(StorageFS *fs, const std::string& dir);

/**
 * Checks if the input directory is a metadata object.
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param dir The directory to be checked.
 * @return *true* if the directory is a metadata object, and *false* otherwise.
 */
bool is_metadata(StorageFS *fs, const std::string& dir);

/** Returns *true* if the input string is a positive (>0) integer number. */
bool is_positive_integer(const char* s);

/** Returns *true* if the subarray contains a single element. */
template<class T>
bool is_unary_subarray(const T* subarray, int dim_num);

/**
 * Checks if the input directory is a workspace.
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param dir The directory to be checked.
 * @return *true* if the directory is a workspace, and *false* otherwise.
 */
bool is_workspace(StorageFS *fs, const std::string& dir);

#ifdef HAVE_MPI
/**
 * Reads data from a file into a buffer using MPI-IO.
 *
 * @param mpi_comm The MPI communicator.
 * @param filename The name of the file.
 * @param offset The offset in the file from which the read will start.
 * @param buffer The buffer into which the data will be written.
 * @param length The size of the data to be read from the file.
 * @return TILEDB_UT_OK on success and TILEDB_UT_ERR on error.
 */
int mpi_io_read_from_file(
    const MPI_Comm* mpi_comm,
    const std::string& filaname,
    off_t offset,
    void* buffer,
    size_t length);

/**
 * Syncs a file or directory using MPI-IO. If the file/directory does not exist,
 * the function gracefully exits (i.e., it ignores the syncing).
 *
 * @param mpi_comm The MPI communicator.
 * @param filename The name of the file.
 * @return TILEDB_UT_OK on success and TILEDB_UT_ERR on error.
 */
int mpi_io_sync(
    const MPI_Comm* mpi_comm,
    const char* filaname);

/** 
 * Writes the input buffer to a file using MPI-IO.
 * 
 * @param mpi_comm The MPI communicator.
 * @param filename The name of the file.
 * @param buffer The input buffer.
 * @param buffer_size The size of the input buffer.
 * @return TILEDB_UT_OK on success, and TILEDB_UT_ERR on error.
 */
int mpi_io_write_to_file(
    const MPI_Comm* mpi_comm,
    const char* filename,
    const void* buffer, 
    size_t buffer_size);
#endif

#ifdef HAVE_OPENMP
/**
 * Destroys an OpenMP mutex.
 *
 * @param mtx The mutex to be destroyed.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
int mutex_destroy(omp_lock_t* mtx);

/**
 * Initializes an OpenMP mutex.
 *
 * @param mtx The mutex to be initialized.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
int mutex_init(omp_lock_t* mtx);

/**
 * Locks an OpenMP mutex.
 *
 * @param mtx The mutex to be locked.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
int mutex_lock(omp_lock_t* mtx);

/**
 * Unlocks an OpenMP mutex.
 *
 * @param mtx The mutex to be unlocked.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
int mutex_unlock(omp_lock_t* mtx);
#endif

/**
 * Destroys a pthread mutex.
 *
 * @param mtx The mutex to be destroyed.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
int mutex_destroy(pthread_mutex_t* mtx);

/**
 * Initializes a pthread mutex.
 *
 * @param mtx The mutex to be initialized.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
int mutex_init(pthread_mutex_t* mtx);

/**
 * Locks a pthread mutex.
 *
 * @param mtx The mutex to be locked.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
int mutex_lock(pthread_mutex_t* mtx);

/**
 * Unlocks a pthread mutex.
 *
 * @param mtx The mutex to be unlocked.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
int mutex_unlock(pthread_mutex_t* mtx);

/** 
 * Returns the parent directory of the input directory. 
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param dir The input directory.
 * @return The parent directory of the input directory.
 */
std::string parent_dir(StorageFS *fs, const std::string& dir);

/**
 * Reads data from a file into a buffer.
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param filename The name of the file.
 * @param offset The offset in the file from which the read will start.
 * @param buffer The buffer into which the data will be written.
 * @param length The size of the data to be read from the file.
 * @return TILEDB_UT_OK on success and TILEDB_UT_ERR on error.
 */
int read_from_file(StorageFS *fs,
    const std::string& filename,
    off_t offset,
    void* buffer,
    size_t length);

/*
 * Reads an entire file into a buffer after decompressing. Memory is allocated by this
 * routine and a pointer to the buffer and the buffer size are returned. It is the caller's
 * responsibility to free the buffer when it is no longer needed.
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param filename The name of the file.
 * @param buffer Pointer to the allocated buffer for the decompressed data.
 * @param length Pointer to the size of the buffer.
 * @return TILEDB_UT_OK on success and TILEDB_UT_ERR on error.
 */
int read_from_file_after_decompression(StorageFS *fs,
    const std::string& filename,
    void** buffer,
    size_t &buffer_size,
    const int compression);


/**
 * Returns the absolute canonicalized directory path of the input directory.
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param dir The input directory to be canonicalized.
 * @return The absolute canonicalized directory path of the input directory.
 */
std::string real_dir(StorageFS *fs, const std::string& dir);

/**
 * Compresses with RLE. 
 *
 * @param input The input buffer to be compressed.
 * @param input_size The size of the input buffer.
 * @param output The output buffer that results from compression.
 * @param output_allocated_size The allocated size of the output buffer.
 * @param value_size The size of each single value in the input buffer. 
 * @return The size of the result ouput buffer upon success, and TILEDB_UT_ERR
 *     on error.
 */
int64_t RLE_compress(
    const unsigned char* input,
    size_t input_size,
    unsigned char* output,
    size_t output_allocated_size,
    size_t value_size);

/**
 * Returns the maximum size of the output of RLE compression.
 *
 * @param input_size The input buffer size.
 * @param value_size The size of a sinlge value in the input buffer.
 * @return The maximum size of the output after RLE-compressing the input with
 *     size input_size.
 */
size_t RLE_compress_bound(
    size_t input_size,
    size_t value_size);

/**
 * Returns the maximum size of the output of RLE compression on the coordinates.
 *
 * @param input_size The input buffer size.
 * @param value_size The size of a sinlge value in the input buffer.
 * @param dim_num The number of dimensions/coordinates in a single value.
 * @return The maximum size of the output after RLE-compressing the input with
 *     size input_size.
 */
size_t RLE_compress_bound_coords(
    size_t input_size,
    size_t value_size,
    int dim_num);

/**
 * Compresses the coordinates of a buffer with RLE, assuming that the cells in 
 * input buffer are sorted in column-major order. 
 *
 * @param input The input buffer to be compressed.
 * @param input_size The size of the input buffer.
 * @param output The output buffer that results from compression.
 * @param output_allocated_size The allocated size of the output buffer.
 * @param value_size The size of each single value in the input buffer. 
 * @param dim_num The number of dimensions/coordinates of each cell in the
 *     input buffer.
 * @return The size of the result ouput buffer upon success, and TILEDB_UT_ERR
 *     on error.
 */
int64_t RLE_compress_coords_col(
    const unsigned char* input,
    size_t input_size,
    unsigned char* output,
    size_t output_allocated_size,
    size_t value_size,
    int dim_num);

/**
 * Compresses the coordinates of a buffer with RLE, assuming that the cells in 
 * input buffer are sorted in row-major order. 
 *
 * @param input The input buffer to be compressed.
 * @param input_size The size of the input buffer.
 * @param output The output buffer that results from compression.
 * @param output_allocated_size The allocated size of the output buffer.
 * @param value_size The size of each single value in the input buffer. 
 * @param dim_num The number of dimensions/coordinates of each cell in the
 *     input buffer.
 * @return The size of the result ouput buffer upon success, and TILEDB_UT_ERR
 *     on error.
 */
int64_t RLE_compress_coords_row(
    const unsigned char* input,
    size_t input_size,
    unsigned char* output,
    size_t output_allocated_size,
    size_t value_size,
    int dim_num);

/**
 * Decompresses with RLE. 
 *
 * @param input The input buffer to be decompressed.
 * @param input_size The size of the input buffer.
 * @param output The output buffer that results from decompression.
 * @param output_allocated_size The allocated size of the output buffer.
 * @param value_size The size of each single value in the input buffer. 
 * @return TILEDB_UT_OK on success and TILEDB_UT_ERR on error.
 */
int RLE_decompress(
    const unsigned char* input,
    size_t input_size,
    unsigned char* output,
    size_t output_allocated_size,
    size_t value_size);

/**
 * Decompresses the coordinates of a buffer with RLE, assuming that the cells in
 * input buffer are sorted in column-major order. 
 *
 * @param input The input buffer to be decompressed.
 * @param input_size The size of the input buffer.
 * @param output The output buffer that results from decompression.
 * @param output_allocated_size The allocated size of the output buffer.
 * @param value_size The size of each single value in the output buffer. 
 * @param dim_num The number of dimensions/coordinates of each cell in the
 *     output buffer.
 * @return TILEDB_UT_OK on success and TILEDB_UT_ERR on error.
 */
int RLE_decompress_coords_col(
    const unsigned char* input,
    size_t input_size,
    unsigned char* output,
    size_t output_allocated_size,
    size_t value_size,
    int dim_num);

/**
 * Decompresses the coordinates of a buffer with RLE, assuming that the cells in
 * input buffer are sorted in row-major order. 
 *
 * @param input The input buffer to be decompressed.
 * @param input_size The size of the input buffer.
 * @param output The output buffer that results from decompression.
 * @param output_allocated_size The allocated size of the output buffer.
 * @param value_size The size of each single value in the output buffer. 
 * @param dim_num The number of dimensions/coordinates of each cell in the
 *     output buffer.
 * @return TILEDB_UT_OK on success and TILEDB_UT_ERR on error.
 */
int RLE_decompress_coords_row(
    const unsigned char* input,
    size_t input_size,
    unsigned char* output,
    size_t output_allocated_size,
    size_t value_size,
    int dim_num);

/** 
 * Checks if a string starts with a certain prefix.
 *
 * @param value The base string.
 * @param prefix The prefix string to be tested.
 * @return *true* if *value* starts with the *prefix*, and *false* otherwise. 
 */
bool starts_with(const std::string& value, const std::string& prefix);

/** 
 * Syncs a file or directory. If the file/directory does not exist,
 * the function gracefully exits (i.e., it ignores the syncing).
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param filename The name of the file.
 * @return TILEDB_UT_OK on success, and TILEDB_UT_ERR on error.
 */
int sync_path(StorageFS *fs, const std::string& path);

/** 
 * Closes any open file handles associated with a file. If the file does not exist,
 * or if there are no open file handles it is a noop).
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param filename The name of the file.
 * @return TILEDB_UT_OK on success, and TILEDB_UT_ERR on error.
 */
int close_file(StorageFS *fs, const std::string& filename);

/** 
 * Writes the input buffer to a file.
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param filename The name of the file.
 * @param buffer The input buffer.
 * @param buffer_size The size of the input buffer.
 * @return TILEDB_UT_OK on success, and TILEDB_UT_ERR on error.
 */
int write_to_file(StorageFS *fs,
                  const std::string& filename,
                  const void* buffer, 
                  size_t buffer_size);

/** 
 * Writes the input buffer after compression to a file.
 *
 * @param fs The storage filesystem type in use. e.g. posix, hdfs, etc.
 * @param filename The name of the file.
 * @param buffer The input buffer.
 * @param buffer_size The size of the input buffer.
 * @return TILEDB_UT_OK on success, and TILEDB_UT_ERR on error.
 */
int write_to_file_after_compression(StorageFS *fs,
                                    const std::string& filename,
                                    const void* buffer,
                                    size_t buffer_size,
                                    const int compression);


/** 
 * Write the input buffer to a file, compressed with GZIP.
 * 
 * @param filename The name of the file.
 * @param buffer The input buffer.
 * @param buffer_size The size of the input buffer.
 * @return TILEDB_UT_OK on success, and TILEDB_UT_ERR on error.
 */
/* TODO: Dead Code
int write_to_file_cmp_gzip(
    const char* filename,
    const void* buffer, 
    size_t buffer_size);
*/

/**
 * Delete directories
 *
 * @param vector of directory paths
 * @return TILEDB_UT_OK on success, and TILEDB_UT_ERR on error.
 */
int delete_directories(StorageFS *fs, const std::vector<std::string>& directories);

/**
 * Move(Rename) Path.
 *
 * @param original path to be moved
 * @param name of new path
 * @return TILEDB_UT_OK on success and TILEDB_UT_ERR on error.
 */
int move_path(StorageFS *fs, const std::string& old_path, const std::string& new_path);

/*
 * Return the TileDB empty value for the datatype
 */
template<class T>
inline T get_tiledb_empty_value();

//Template specialization for get_tiledb_empty_value()
template<>
inline int get_tiledb_empty_value()
{
  return TILEDB_EMPTY_INT32;
}

template<>
inline int64_t get_tiledb_empty_value()
{
  return TILEDB_EMPTY_INT64;
}

template<>
inline float get_tiledb_empty_value()
{
  return TILEDB_EMPTY_FLOAT32;
}

template<>
inline double get_tiledb_empty_value()
{
  return TILEDB_EMPTY_FLOAT64;
}

template<>
inline char get_tiledb_empty_value()
{
  return TILEDB_EMPTY_CHAR;
}

#endif
