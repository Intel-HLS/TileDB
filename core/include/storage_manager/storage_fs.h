/**
 * @file storage_fs.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 Omics Data Automation Inc. and Intel Corporation
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
 * Storage API exposes some filesystem specific functionality implemented in TileDB.
 *
 */

#ifndef __STORAGE_FS_H__
#define  __STORAGE_FS_H__

#include <string>
#include <vector>
#include <sys/stat.h>

/**@{*/
/** Return code. */
#define TILEDB_FS_OK         0
#define TILEDB_FS_ERR       -1
/**@}*/

/** Default error message. */
#define TILEDB_FS_ERRMSG std::string("[TileDB::FileSystem] Error: ")

/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

/** Stores potential error messages. */
extern std::string tiledb_fs_errmsg;

/** Base Class for Filesystems */
class StorageFS {
 public:
  virtual ~StorageFS();

  virtual std::string current_dir() = 0;
  
  virtual bool is_dir(const std::string& dir) = 0;
  virtual bool is_file(const std::string& file) = 0;
  virtual std::string real_dir(const std::string& dir) = 0;
  
  virtual int create_dir(const std::string& dir) = 0;
  virtual int delete_dir(const std::string& dir) = 0;

  virtual std::vector<std::string> get_dirs(const std::string& dir) = 0;
  virtual std::vector<std::string> get_files(const std::string& dir) = 0;

  virtual int create_file(const std::string& filename, int flags, mode_t mode) = 0;
  virtual int delete_file(const std::string& filename) = 0;

  virtual size_t file_size(const std::string& filename) = 0;

  virtual int read_from_file(const std::string& filename, off_t offset, void *buffer, size_t length) = 0;
  virtual int write_to_file(const std::string& filename, const void *buffer, size_t buffer_size) = 0;

  virtual int move_path(const std::string& old_path, const std::string& new_path) = 0;
    
  virtual int sync_path(const std::string& path) = 0;

  virtual int close_file(const std::string& filename);

  virtual bool locking_support();
};

#endif /* __STORAGE_FS_H__ */

