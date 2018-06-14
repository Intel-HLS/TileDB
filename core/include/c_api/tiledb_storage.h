/**
 * @file tiledb_storage.h
 *
 * @section LICENSE
 *
 * The MIT License
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
 * Storage Prototypes to expose some of the filesystem functionality implemented in TileDB.
 * The implementation is in tiledb.cc.
 *
 */

#ifndef __TILEDB_STORAGE_H__
#define  __TILEDB_STORAGE_H__

#include "tiledb.h"
#include <string>
#include <vector>

/**
 * Checks if the input directory is a workspace.
 *
 * @param tiledb_ctx TileDB Context
 * @param dir The directory to be checked.
 * @return *true* if the directory is a workspace, and *false* otherwise.
 */
bool is_workspace(const TileDB_CTX* tiledb_ctx, const std::string& dir);

/**
 * Checks if the input directory is a group.
 *
 * @param tiledb_ctx TileDB Context
 * @param dir The directory to be checked.
 * @return *true* if the directory is a group, and *false* otherwise.
 */
bool is_group(const TileDB_CTX* tiledb_ctx, const std::string& dir);

/**
 * Checks if the input directory is an array.
 *
 * @param tiledb_ctx TileDB Context
 * @param dir The directory to be checked.
 * @return *true* if the directory is an array, and *false* otherwise.
 */
bool is_array(const TileDB_CTX* tiledb_ctx, const std::string& dir);

/**
 * Checks if the input directory is a fragment.
 *
 * @param tiledb_ctx TileDB Context
 * @param dir The directory to be checked.
 * @return *true* if the directory is a fragment, and *false* otherwise.
 */
bool is_fragment(const TileDB_CTX* tiledb_ctx, const std::string& dir);

/**
 * Checks if the input directory is a metadata object.
 *
 * @param tiledb_ctx TileDB Context
 * @param dir The directory to be checked.
 * @return *true* if the directory is a metadata object, and *false* otherwise.
 */
bool is_metadata(const TileDB_CTX* tiledb_ctx, const std::string& dir);

/** 
 * Checks if the input is an existing directory. 
 *
 * @param tiledb_ctx TileDB Context
 * @param dir The directory to be checked.
 * @return *true* if *dir* is an existing directory, and *false* otherwise.
 */ 
bool is_dir(const TileDB_CTX* tiledb_ctx, const std::string& dir);

/** 
 * Checks if the input is an existing file. 
 *
 * @param tiledb_ctx TileDB Context
 * @param file The file to be checked.
 * @return tTrue* if *file* is an existing file, and *false* otherwise.
 */
bool is_file(const TileDB_CTX* tiledb_ctx, const std::string& file);

/** 
 * Return parent directory for given path
 *
 * @param path 
 * @return parent of given path or NULL
 */
std::string parent_dir(const std::string& path);

/**
 */
std::string parent_dir(const std::string& path);

/**
 * Creates a new directory.
 *
 * @param tiledb_ctx TileDB Context
 * @param dir The name of the directory to be created.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error. 
 */
int create_dir(const TileDB_CTX* tiledb_ctx, const std::string& dir);

/**
 * Deletes a directory. Note that the directory must not contain other
 * directories, but it should only contain files.
 *
 * @param tiledb_ctx TileDB Context
 * @param dirname The name of the directory to be deleted.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error. 
 */
int delete_dir(const TileDB_CTX* tiledb_ctx, const std::string& dir);

/** Returns the names of the directories inside the input directory.
 *
 * @param tiledb_ctx TileDB Context
 * @param dir The input directory.
 * @return The vector of directories contained in the input directory.
 */  
std::vector<std::string> get_dirs(const TileDB_CTX* tiledb_ctx, const std::string& dir);
 
/** Returns the names of the files inside the input directory.
 *
 * @param tiledb_ctx TileDB Context
 * @param dir The input directory.
 * @return The vector of directories contained in the input directory.
 */
std::vector<std::string> get_files(const TileDB_CTX* tiledb_ctx, const std::string& dir);

/** 
 * Returns the size of the input file.
 *
 * @param tiledb_ctx TileDB Context
 * @param filename The name of the file whose size is to be retrieved.
 * @return The file size on success, and TILEDB_UT_ERR for error.
 */
size_t file_size(const TileDB_CTX* tiledb_ctx, const std::string& file);

/**
 * Reads data from a file into a buffer.
 *
 * @param tiledb_ctx TileDB Context
 * @param filename The name of the file.
 * @param offset The offset in the file from which the read will start.
 * @param buffer The buffer into which the data will be written.
 * @param length The size of the data to be read from the file.
 * @return TILEDB_UT_OK on success and TILEDB_UT_ERR on error.
 */
int read_file(const TileDB_CTX* tiledb_ctx, const std::string& filename, off_t offset, void *buffer, size_t length);

/** 
 * Writes the input buffer to a file.
 *
 * @param tiledb_ctx TileDB Context
 * @param filename The name of the file.
 * @param buffer The input buffer.
 * @param buffer_size The size of the input buffer.
 * @return TILEDB_UT_OK on success, and TILEDB_UT_ERR on error.
 */
int write_file(const TileDB_CTX* tiledb_ctx, const std::string& filename, const void *buffer, size_t buffer_size);

/**
 * Deletes a file from the filesystem
 *
 * @param tiledb_ctx TileDB Context
 * @param filename The name of the file to be deleted.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error. 
 */
int delete_file(const TileDB_CTX* tiledb_ctx, const std::string& filename);

/** 
 * Closes any open file handles associated with a file. If the file does not exist,
 * or if there are no open file handles it is a noop).
 *
 * @param tiledb_ctx TileDB Context
 * @param filename The name of the file.
 * @return TILEDB_UT_OK on success, and TILEDB_UT_ERR on error.
 */
int close_file(const TileDB_CTX* tiledb_ctx, const std::string& filename);

#endif /*  __TILEDB_STORAGE_H__ */
