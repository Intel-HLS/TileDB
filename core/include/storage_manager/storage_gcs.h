/**
 * @ storage_gcs.h
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
 * GCS derived from StorageFS for Google Cloud Services
 *
 */

#ifndef __STORAGE_GCS_H__
#define  __STORAGE_GCS_H__

#include "storage_fs.h"

#ifdef USE_HDFS

#include "cloud_storage_prototypes.h"
#include "tiledb_constants.h"

#include <mutex>
#include <unordered_map>

extern void load_hdfs_library();

class GCS : public StorageFS {
 public:
  GCS(const std::string& home);
  ~GCS();

  std::string current_dir();

  bool is_dir(const std::string& dir);
  bool is_file(const std::string& file);
  std::string real_dir(const std::string& dir);
               
  int create_dir(const std::string& dir);
  int delete_dir(const std::string& dir);

  std::vector<std::string> get_dirs(const std::string& dir);
  std::vector<std::string> get_files(const std::string& dir);
  
  int create_file(const std::string& filename, int flags, mode_t mode);
  int delete_file(const std::string& filename);

  size_t file_size(const std::string& filename);

  int read_from_file(const std::string& filename, off_t offset, void *buffer, size_t length);
  int write_to_file(const std::string& filename, const void *buffer, size_t buffer_size);
  
  int move_path(const std::string& old_path, const std::string& new_path);
    
  int sync_path(const std::string& path);

  int close_file(const std::string& filename);

  bool consolidation_support();

  private:
  hdfsFS hdfs_handle_ = NULL;
  std::mutex read_map_mtx_, write_map_mtx_;
  std::unordered_map<std::string, hdfsFile> read_map_, write_map_;
  std::unordered_map<std::string, int> read_count_;
};

#else

class GCS : public StorageFS {

 public:
  GCS(const std::string& home) {};
  ~GCS() {};

  GCS(const char *home) {};
};

#endif


#endif /* __STORAGE_GCS_H__ */

