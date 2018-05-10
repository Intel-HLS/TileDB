/**
 * @file storage_gcs.cc
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
 * GCS Support for StorageFS
 */

#include "storage_gcs.h"

#ifdef USE_HDFS

#include "storage_posixfs.h"
#include "utils.h"

#include <assert.h>
#include <cerrno>
#include <cstring>
#include <clocale>
#include <iostream>
#include <memory>
#include <cstdlib>
#include <unistd.h>

#ifdef TILEDB_VERBOSE
#  define PRINT_ERROR(x) std::cerr << TILEDB_FS_ERRMSG << "hdfs: " << x << std::endl
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif

// Forward declarations
static void invoke_function(hdfsFS hdfs_handle, std::unordered_map<std::string, hdfsFile>& map, int (*fn)(hdfsFS, hdfsFile, const std::string&));
static int sync_kernel(hdfsFS hdfs_handle, hdfsFile hdfs_file_handle, const std::string& path);
static int close_kernel(hdfsFS hdfs_handle, hdfsFile hdfs_file_handle, const std::string& path);

#include <sys/time.h>
#include <sys/resource.h>
char *trim(char *value) {
  while (value[0] == '\"' || value[0] == ' ' || value[0] == '\n') {
    value++;
  } 
  while (value[strlen(value)-1] == '\"' || value[strlen(value)-1] == ' ' || value[strlen(value)-1] == '\n') {
    value[strlen(value)-1] = 0;
  }
  if (strcmp(value, "{") == 0 || strcmp(value, "}") == 0) {
    return NULL;
  }
  return value;
}

char *parse_json(char *filename, const char *key) {
  PosixFS *fs = new PosixFS();
  size_t size;

  if (!fs->is_file(filename) || (size = fs->file_size(filename)) <= 0) {
    return NULL;
  }

  char *buffer = (char *)malloc(size + 1);
  assert(buffer && "Failed to allocate buffer");
  memset((void *)buffer, 0, size+1);

  int rc = fs->read_from_file(filename, 0, buffer, size);
  assert(!rc && "Failure to read file");

  delete fs;
  
  char *value = NULL;
  char *saveptr;
  char *token = strtok_r(buffer, ",\n ", &saveptr);
  while (token) {
    token = trim(token);
    if (token) {
      char *sub_saveptr;
      char *sub_token = strtok_r(token, ":", &sub_saveptr);
      if (strcmp(trim(sub_token), key) == 0) {
	value = strtok_r(NULL, ":", &sub_saveptr);
	free(buffer);
	value = trim(value);
	if (value) {
	  return strdup(value);
	} else {
	  return NULL;
	}
      }
    }
    token = strtok_r(NULL, ",", &saveptr);
  }
  free(buffer);
  return NULL;
}

#define GCS_PREFIX "gs://"

GCS::GCS(const std::string& home) {
  load_hdfs_library();
  struct hdfsBuilder *builder = hdfsNewBuilder();
  if (!builder) {
    assert(false && "Error getting new hdfs builder instance");
  }

  hdfsBuilderSetForceNewInstance(builder);

  assert(starts_with(home, GCS_PREFIX) && "Home URL not supported");

  std::string gcs_bucket;
  std::string working_dir;
  auto n = home.find("/", strlen(GCS_PREFIX));
  if (n == std::string::npos){
    gcs_bucket = home;
    working_dir.assign("/");
  } else {
    gcs_bucket = home.substr(0, n);
    working_dir.assign(home.substr(n));
  }
  assert(!gcs_bucket.empty() && "GCS bucket name could not be deduced from home URL");

  hdfsBuilderSetNameNode(builder, gcs_bucket.c_str());

  char *gcs_creds = getenv("GOOGLE_APPLICATION_CREDENTIALS");
  char *value = NULL;
  if (gcs_creds) {
    value = parse_json(gcs_creds, "project_id"); // free value after hdfsBuilderConnect as it is shallow copied
    if (value) {
      hdfsBuilderConfSetStr(builder, "google.cloud.auth.service.account.enable", "true");
      hdfsBuilderConfSetStr(builder, "google.cloud.auth.service.account.json.keyfile", gcs_creds);
      hdfsBuilderConfSetStr(builder, "fs.gs.project.id", value);
      hdfsBuilderConfSetStr(builder, "fs.gs.working.dir", working_dir.c_str());
    }
  }
    
  hdfs_handle_ = hdfsBuilderConnect(builder);
  if (!hdfs_handle_) {
    assert(false && "Error with hdfs builder connection");
  }

  if (value) {
    free(value);
  }

  hdfsSetWorkingDirectory(hdfs_handle_, working_dir.c_str());
}

GCS::~GCS() {
  // Close any files that have been opened and not closed.
  invoke_function(hdfs_handle_, read_map_, &close_kernel);
  read_map_.clear();
  
  invoke_function(hdfs_handle_, write_map_, &sync_kernel);
  invoke_function(hdfs_handle_, write_map_, &close_kernel);
  write_map_.clear(); 

  if (hdfsDisconnect(hdfs_handle_)) {
    PRINT_ERROR("disconnect error");
  }
  
  hdfs_handle_ = NULL;
}

static int print_errmsg(const std::string& errmsg) {
  if (errmsg.length() > 0) {
    PRINT_ERROR(errmsg);
    tiledb_fs_errmsg = TILEDB_FS_ERRMSG + errmsg;
  }
  return TILEDB_FS_ERR;
}

static hdfsFile get_hdfsFile(const std::string& filename, std::unordered_map<std::string, hdfsFile>& map) {
  auto search = map.find(filename);
  if (search != map.end()) {
    return search->second;
  } else {
    return NULL;
  }
}

static int close_read_hdfsFile(hdfsFS hdfs_handle, const std::string& filename, std::unordered_map<std::string, hdfsFile>& map, std::unordered_map<std::string, int>& count_map, std::mutex& mtx) {
  int rc = TILEDB_FS_OK;

  mtx.lock();
  hdfsFile hdfs_file_handle = get_hdfsFile(filename, map);
  if (hdfs_file_handle) {
    int count = 0;
    auto search = count_map.find(filename);
    if (search != count_map.end()) {
      count =  search->second;
    }
    if (count == 0) {
      rc = close_kernel(hdfs_handle, hdfs_file_handle, filename);
      map.erase(filename);
    }
  }
  mtx.unlock();

  return rc;
}

static int close_write_hdfsFile(hdfsFS hdfs_handle, const std::string& filename, std::unordered_map<std::string, hdfsFile>& map, std::mutex& mtx) {
  int rc = TILEDB_FS_OK;

  mtx.lock();
  hdfsFile hdfs_file_handle = get_hdfsFile(filename, map);
  if (hdfs_file_handle) {
    sync_kernel(hdfs_handle, hdfs_file_handle, filename);
    rc = close_kernel(hdfs_handle, hdfs_file_handle, filename);
  }
  map.erase(filename);
  mtx.unlock();

  return rc;
}

static void invoke_function(hdfsFS hdfs_handle, std::unordered_map<std::string, hdfsFile>& map, int (*fn)(hdfsFS, hdfsFile, const std::string&)) {
  for (auto it = map.begin(); it != map.end(); ++it) {
    std::string filename = it->first;
    hdfsFile hdfs_file_handle = it->second;

    // Call function kernel
    fn(hdfs_handle, hdfs_file_handle, filename);
  }
}

std::string GCS::current_dir() {
  char working_dir[TILEDB_NAME_MAX_LEN];
  if (hdfsGetWorkingDirectory(hdfs_handle_, working_dir, TILEDB_NAME_MAX_LEN) == NULL) {
    print_errmsg("Could not get current working dir");
    return NULL;
  }
  std::string cwd = working_dir;
  return working_dir;
}

static bool is_path(const hdfsFS hdfs_handle, const char *path, const char kind) {
  if (!hdfsExists(hdfs_handle, path)) {
    hdfsFileInfo *file_info = hdfsGetPathInfo(hdfs_handle, path);
    if (file_info) {
      bool status = false;
      if ((char)(file_info->mKind) == kind) {
        status = true;
      }
      hdfsFreeFileInfo(file_info, 1);
      return status;
    }
  }
  return false;
}

bool GCS::is_dir(const std::string& dir) {
  std::string slash("/");
  if (dir.back() != '/') {
    return is_path(hdfs_handle_, (dir + slash).c_str(), 'D');
  }
  return is_path(hdfs_handle_, dir.c_str(), 'D');
}

bool GCS::is_file(const std::string& file) {
  return is_path(hdfs_handle_, file.c_str(), 'F');
}

std::string GCS::real_dir(const std::string& dir) {
  if (dir.empty()) {
    return current_dir();
  } else if (is_gcs_path(dir)) {
    // absolute path
    return dir;
  } else if (starts_with(dir, "/")) {
    // seems to be an absolute path but without protocol/host information.
    print_errmsg(dir + ": Not a valid GCS path");
    assert(false && "Not a valid GCS path");
  } else {
    // relative path
    return current_dir() + "/" + dir;
  }
}
  
int GCS::create_dir(const std::string& dir) {
  if (is_dir(dir)) {
    return print_errmsg(std::string("Cannot create directory ") + dir + "; Directory already exists");
  }

  if (hdfsCreateDirectory(hdfs_handle_, dir.c_str()) < 0) {
    return print_errmsg(std::string("Cannot create directory ") + dir);
  }
    
  return TILEDB_FS_OK;
}

int GCS::delete_dir(const std::string& dir) {
  if (is_dir(dir)) {
    if (hdfsDelete(hdfs_handle_, dir.c_str(), 1) < 0) {
      return print_errmsg(std::string("Cannot delete directory ") + dir);
    }
  }else {
    return print_errmsg(std::string("Cannot delete path at ") + dir);
  }
  
  return TILEDB_FS_OK;
}

std::vector<std::string> GCS::get_dirs(const std::string& dir) {
  std::vector<std::string> path_list;

  int num_entries = 0;
  hdfsFileInfo *file_info = hdfsListDirectory(hdfs_handle_, dir.c_str(), &num_entries);
  if (!file_info) {
    print_errmsg(std::string("Cannot list contents of dir ") + dir);
  } else {
    for (int i=0; i<num_entries; i++) {
      if (file_info[i].mKind == 'D') {
        path_list.push_back(std::string(file_info[i].mName));
      }
    }
  }
  hdfsFreeFileInfo(file_info, 1);

  return path_list;
}
    
std::vector<std::string> GCS::get_files(const std::string& dir) {
   std::vector<std::string> path_list;

  int num_entries = 0;
  hdfsFileInfo *file_info = hdfsListDirectory(hdfs_handle_, dir.c_str(), &num_entries);
  if (!file_info) {
    print_errmsg(std::string("Cannot list contents of dir ") + dir);
  } else {
    for (int i=0; i<num_entries; i++) {
      if (file_info[i].mKind == 'F') {
        path_list.push_back(std::string(file_info[i].mName));
      }
    }
  }

  return path_list;
}

int GCS::create_file(const std::string& filename, int flags, mode_t mode) {
  hdfsFile file = hdfsOpenFile(hdfs_handle_, filename.c_str(), O_WRONLY, 0, 0, 0);
  if (!file) {
    return print_errmsg(std::string("Cannot create file ") + filename + "; Open error " + std::strerror(errno));
  }
    
  if (hdfsCloseFile(hdfs_handle_, file)) {
    return print_errmsg(std::string("Cannot create file ") + filename + "; Close error " + std::strerror(errno));
  }
  
  return TILEDB_FS_OK;
}

int GCS::delete_file(const std::string& filename) {
  if (get_hdfsFile(filename, read_map_) || get_hdfsFile(filename, write_map_)) {
    return print_errmsg(std::string("Cannot delete file ") + filename + " as it is open in this context");
  }
  
  if (is_file(filename)) {
    if (hdfsDelete(hdfs_handle_, filename.c_str(), 0) < 0) {
      return print_errmsg(std::string("Cannot delete file ") + filename);
    }
  } else {
    return print_errmsg(std::string("Cannot delete file ") + filename + " as it either does not exist or is not a file");
  }
  
  return TILEDB_FS_OK;
}

size_t GCS::file_size(const std::string& filename) {
  hdfsFileInfo* file_info = hdfsGetPathInfo(hdfs_handle_, filename.c_str());
  if (!file_info) {
    print_errmsg(std::string("Cannot get path info for file ") + filename);
    return 0;
  }

  if ((char)(file_info->mKind) != 'F') {
    print_errmsg(std::string("Cannot get file_size for path ") + filename + " that is not a file");
    hdfsFreeFileInfo(file_info, 1);
    return 0;
  }

  size_t size = (size_t)file_info->mSize;
    
  hdfsFreeFileInfo(file_info, 1);

  return size;
}

static tSize max_tsize() {
  switch (sizeof(tSize)) {
    case 4:
      return INT32_MAX;
    default:
      print_errmsg("hdfs tSize width not recognized in read_from_file");
      return 0;
  }
}

static int read_from_file_kernel(hdfsFS hdfs_handle, hdfsFile file, void* buffer, size_t length, off_t offset) {
  if (hdfsSeek(hdfs_handle, file, (tOffset)offset) < 0) {
    return print_errmsg(std::string("Cannot seek to offset in file"));
  }

  size_t max_bytes = max_tsize();
  if (max_bytes == 0) {
    return TILEDB_FS_ERR;
  }

  size_t nbytes = 0;
  char *pbuf = (char *)buffer;
  do {
    tSize bytes_read = hdfsRead(hdfs_handle, file, (void *)pbuf,  (length - nbytes) > max_bytes ? max_bytes : length - nbytes);
    if (bytes_read < 0) {
      return print_errmsg(std::string("Error reading file. ") + std::strerror(errno));
    }
    nbytes += bytes_read;
    pbuf += bytes_read;
  } while (nbytes < length);

  return TILEDB_FS_OK;
}

static hdfsFile filtered_hdfs_open_file_for_read(hdfsFS hdfs_handle, const std::string& filename, size_t buffer_size) {
  // Workaround for error messages of the type -
  // readDirect: FSDataInputStream#read error:
  // java.lang.UnsupportedOperationException: Byte-buffer read unsupported by input stream
  //	at org.apache.hadoop.fs.FSDataInputStream.read(FSDataInputStream.java:146)
  // This practically litters the output!!
  int old_fd, new_fd;
  fflush(stderr);
  old_fd = dup(2);
  new_fd = open("/dev/null", O_WRONLY);
  dup2(new_fd, 2);
  close(new_fd);
  
  hdfsFile file = hdfsOpenFile(hdfs_handle, filename.c_str(), O_RDONLY, buffer_size, 0, 0);
  
  fflush(stderr);
  dup2(old_fd, 2);
  close(old_fd);

  return file;
}

// heuristic for io.file.buffer.size for good hdfs performance
#define MAX_SIZE 16*1024*1024

static int read_count(const std::string& filename, std::unordered_map<std::string, int>& read_count_map, bool incr) {
  int count = 0;
  auto search = read_count_map.find(filename);
  if (search != read_count_map.end()) {
    count = search->second;
  }
  if (incr) {
    ++count;
  } else {
    --count;
  }
  if (search != read_count_map.end()) {
    search->second = count;
  } else {
    read_count_map.emplace(filename, count);
  }
  return count;
}

int GCS::read_from_file(const std::string& filename, off_t offset, void *buffer, size_t length) {
  // Not supporting simultaneous read/writes.
  if (get_hdfsFile(filename, write_map_)) {
    print_errmsg(std::string("File=") + filename + " is open simultaneously for reads/writes");
    assert(false && "No support for simultaneous reads/writes");
  }

  size_t size = file_size(filename);

  hdfsFile file;

  read_map_mtx_.lock();
  file = get_hdfsFile(filename, read_map_);
  if (!file) {
    file = filtered_hdfs_open_file_for_read(hdfs_handle_, filename, size>MAX_SIZE?MAX_SIZE:((size/getpagesize())+1)*getpagesize());
    if (file) {
      read_map_.emplace(filename, file);
    }
  }
  int count = read_count(filename, read_count_, true);
  assert(count > 0 && "Read File Count cannot be less than 1");
  read_map_mtx_.unlock();

  if (!file) {
    return print_errmsg(std::string("Cannot open file ") + filename + " for read");
  }

  int rc = read_from_file_kernel(hdfs_handle_, file, buffer, length>size?size:length, offset);

  read_map_mtx_.lock();
  count = read_count(filename, read_count_, false);
  assert(count >= 0 && "Read File Count cannot be negative");
  read_map_mtx_.unlock();

  return rc;
}

static int write_to_file_kernel(hdfsFS hdfs_handle, hdfsFile file, const void* buffer, size_t buffer_size, size_t max_bytes) {
  size_t nbytes = 0;
  char *pbuf = (char *)buffer; 
  do {
    tSize bytes_written = hdfsWrite(hdfs_handle, file, (void *)pbuf,  (buffer_size - nbytes) > max_bytes ? max_bytes : buffer_size - nbytes);
    if (bytes_written < 0) {
      return print_errmsg(std::string("Error writing to file"));
    }
    nbytes += bytes_written;
    pbuf += bytes_written;
  } while (nbytes < buffer_size);
    
  if (hdfsFlush(hdfs_handle, file)) {
    return print_errmsg(std::string("Error flushing file ") + std::strerror(errno));
  }

  return TILEDB_FS_OK;
}

int GCS::write_to_file(const std::string& filename, const void *buffer, size_t buffer_size) {
  size_t max_bytes = max_tsize();
  if (max_bytes == 0) {
    return TILEDB_FS_ERR;
  }

  hdfsFile file = get_hdfsFile(filename, write_map_);
  if (!file) {
    write_map_mtx_.lock();
    file = hdfsOpenFile(hdfs_handle_, filename.c_str(), O_WRONLY, 0, 0, 0);
    if (file) {
      write_map_.emplace(filename, file);
    }
    write_map_mtx_.unlock();
    if (!file) {
      return  print_errmsg(std::string("Cannot open file " + filename + " for write"));
    }
  }

  return write_to_file_kernel(hdfs_handle_, file, buffer, buffer_size, max_bytes);
}

int GCS::move_path(const std::string& old_path, const std::string& new_path) {
  if (!hdfsExists(hdfs_handle_, new_path.c_str())) {
    return print_errmsg(std::string("Cannot move path ") + old_path + " to " + new_path + " as it exists");
  }
    
  if (hdfsRename(hdfs_handle_, old_path.c_str(), new_path.c_str()) < 0) {
    return print_errmsg(std::string("Cannot rename path ") + old_path + " to " + new_path);
  }

  return TILEDB_FS_OK;
}
    
static int sync_kernel(hdfsFS hdfs_handle, hdfsFile hdfs_file_handle, const std::string& path) {
  if (hdfsHSync(hdfs_handle, hdfs_file_handle)) {
    return print_errmsg(std::string("Cannot sync file ") + path);
  }

  return TILEDB_FS_OK;
}

int GCS::sync_path(const std::string& path) {
  int rc = TILEDB_FS_OK;
  
  auto search = write_map_.find(path);
  if(search != write_map_.end()) {
    rc = sync_kernel(hdfs_handle_, search->second, path);
  }

  return rc;
}

static int close_kernel(hdfsFS hdfs_handle, hdfsFile hdfs_file_handle, const std::string& filename) {
  if (hdfsCloseFile(hdfs_handle, hdfs_file_handle)) {
    return print_errmsg(std::string("Cannot close file ") + filename);
  }
  return TILEDB_FS_OK;
}

int GCS::close_file(const std::string& filename) {
  int rc_close_read = TILEDB_FS_OK, rc_close_write=TILEDB_FS_OK;
  rc_close_read = close_read_hdfsFile(hdfs_handle_, filename, read_map_, read_count_, read_map_mtx_);
  rc_close_write = close_write_hdfsFile(hdfs_handle_, filename, write_map_, write_map_mtx_);

  if (rc_close_read) {
    return rc_close_read;
  }

  return rc_close_write;
}

static bool done_printing_consolidation_support_message = false;

bool GCS::consolidation_support() {
  if (!done_printing_consolidation_support_message) {
    print_errmsg("TBD: No consolidation locking support for cloud file systems.");
    done_printing_consolidation_support_message = true;
  }
  return false;
}

#endif
