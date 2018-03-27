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

bool is_hdfs_path(const std::string& path);

bool is_workspace(const TileDB_CTX* tiledb_ctx, const std::string& dir);

bool is_group(const TileDB_CTX* tiledb_ctx, const std::string& dir);

bool is_array(const TileDB_CTX* tiledb_ctx, const std::string& dir);

bool is_fragment(const TileDB_CTX* tiledb_ctx, const std::string& dir);

bool is_metadata(const TileDB_CTX* tiledb_ctx, const std::string& dir);

bool is_dir(const TileDB_CTX* tiledb_ctx, const std::string& dir);

bool is_file(const TileDB_CTX* tiledb_ctx, const std::string& file);

std::string parent_dir(const std::string& path);

size_t file_size(const TileDB_CTX* tiledb_ctx, const std::string& file);

int read_from_file(const TileDB_CTX* tiledb_ctx, const std::string& filename, off_t offset, void *buffer, size_t length);

int write_to_file(const TileDB_CTX* tiledb_ctx, const std::string& filename, const void *buffer, size_t buffer_size);

int delete_file(const TileDB_CTX* tiledb_ctx, const std::string& filename);

int close_file(const TileDB_CTX* tiledb_ctx, const std::string& filename);

#endif /*  __TILEDB_STORAGE_H__ */
