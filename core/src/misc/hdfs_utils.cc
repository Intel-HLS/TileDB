/**
 * @file   hdfs_utils.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 Omics Data Automation, Inc.
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
 * HDFS Utilities
 */

#include "tiledb.h"
#include "hdfs_utils.h"

#include <assert.h>

#ifdef USE_HDFS

#include "url.h"

#include <cstdint>
#include <iostream>
#include <hdfs.h>
#include <limits.h>
#include <string.h>
#include <thread>

#include "trace.h"

#ifdef TILEDB_VERBOSE
#  define PRINT_ERROR(x) std::cerr << "hdfs: " << TILEDB_UT_ERRMSG << x << ".\n"
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif

namespace hdfs {
static thread_local hdfsFS hdfs_handle = NULL;
static thread_local char home_dir[TILEDB_NAME_MAX_LEN+1];

#define ERR_MSG_LEN 2048
static char errmsg[ERR_MSG_LEN+1];

static int print_errmsg() {
  errmsg[ERR_MSG_LEN] = 0;
  if (strlen(errmsg) > 0) {
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg;
  }
  return TILEDB_UT_ERR;
}

void set_hdfs_handle(hdfsFS handle) {
  hdfs_handle = handle;
}

int connect(const std::string& path) {
  struct hdfsBuilder *builder = hdfsNewBuilder();
  if (!builder) {
    snprintf(errmsg, ERR_MSG_LEN, "error getting new builder instance");
    return print_errmsg();
  }

  hdfsBuilderSetForceNewInstance(builder);

  url path_url(path);
  // protocol_nn needs to be valid in this scope. Moving it inside the if-else
  // below will cause a subtle bug where protocol_nn goes out of scope
  // by the time hdfsConnect tries to use protocol_nn
  std::string protocol_nn = path_url.protocol()+"://"+path_url.host();
  assert(!path_url.protocol().compare("hdfs") || !path_url.protocol().compare("s3") || !path_url.protocol().compare("gs"));
  if (path_url.host().empty()) {
    if (!path_url.port().empty()) {
      snprintf(errmsg, ERR_MSG_LEN, "path=%s not supported. hdfs host and port have to be both empty", path.c_str());
      return print_errmsg();
    }
    hdfsBuilderSetNameNode(builder, "default");
  } else if(!path_url.protocol().compare("s3") || !path_url.protocol().compare("gs")) {
    hdfsBuilderSetNameNode(builder, protocol_nn.c_str());
  } else {
    if (path_url.port().empty()) {
      snprintf(errmsg, ERR_MSG_LEN, "path=%s not supported. hdfs host and port have to be specified together", path.c_str());
      return print_errmsg();
    }
    hdfsBuilderSetNameNode(builder, path_url.host().c_str());
    hdfsBuilderSetNameNodePort(builder, path_url.nport());
  }

  hdfs_handle = hdfsBuilderConnect(builder);
  if (!hdfs_handle) {
    snprintf(errmsg, ERR_MSG_LEN, "builder connect error");
    return print_errmsg();
  }

  hdfsSetWorkingDirectory(hdfs_handle, path.c_str());
  hdfsGetWorkingDirectory(hdfs_handle, home_dir, TILEDB_NAME_MAX_LEN);

  return TILEDB_UT_OK;
}


void disconnect() {
  if (hdfsDisconnect(hdfs_handle)) {
    snprintf(errmsg, ERR_MSG_LEN, "disconnect error");
    print_errmsg();
  }
  hdfs_handle = NULL;
}

bool is_hdfs_path(const std::string& path) {
  return starts_with(path, "hdfs://") || starts_with(path, "s3://") || starts_with(path, "gs://");
}

bool is_hdfs() {
  return hdfs_handle != NULL;
}

char *get_home_dir() {
  return home_dir;
}

std::string current_dir() {
  char working_dir[TILEDB_NAME_MAX_LEN];
  if (hdfsGetWorkingDirectory(hdfs_handle, working_dir, TILEDB_NAME_MAX_LEN) == NULL) {
    snprintf(errmsg, ERR_MSG_LEN, "could not get current working dir");
    print_errmsg();
    return NULL;
  }
  std::string cwd = working_dir;
  return working_dir;
}

static bool is_path(const char *path, const char kind) {
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

bool is_dir(const std::string& dir) {
  return is_path(dir.c_str(), 'D');
}

bool is_file(const std::string& file) {
  return is_path(file.c_str(), 'F');
}

std::string real_dir(const std::string& dir) {
  if (is_hdfs_path(dir)) {
    return dir;
  } else if (dir == "") {
    return current_dir();
  } else if (!starts_with(dir, "/")) {
    return current_dir() + "/" + dir;
  } else {
     snprintf(errmsg, ERR_MSG_LEN, "Cannot evaluate %s to a real_dir. Aborting...", dir.c_str());
     assert(false);
  }
}

int create_dir(const std::string& dir) {
  if (is_dir(dir)) {
    snprintf(errmsg, ERR_MSG_LEN, "cannot create directory %s as it already exists", dir.c_str());
    return print_errmsg();
  }

  if (hdfsCreateDirectory(hdfs_handle, dir.c_str()) < 0) {
    snprintf(errmsg, ERR_MSG_LEN, "cannot create directory %s", dir.c_str());
    return print_errmsg();
  }
    
  return TILEDB_UT_OK;
}

int delete_dir(const std::string& dir) {
  if (is_dir(dir)) {
    if (hdfsDelete(hdfs_handle, dir.c_str(), 1) < 0) {
      snprintf(errmsg, ERR_MSG_LEN, "cannot delete directory %s", dir.c_str());
      return print_errmsg();
    }
  }

  return TILEDB_UT_OK;
}

std::vector<std::string> get_dirs(const std::string&dir) {
  std::vector<std::string> path_list;

  int num_entries = 0;
  hdfsFileInfo *file_info = hdfsListDirectory(hdfs_handle, dir.c_str(), &num_entries);
  if (!file_info) {
    snprintf(errmsg, ERR_MSG_LEN, "cannot list contents of dir %s", dir.c_str());
    print_errmsg();
  } else {
    for (int i=0; i<num_entries; i++) {
      if (file_info[i].mKind == 'D') {
        path_list.push_back(std::string(file_info[i].mName));
      }
    }
  }

  return path_list;
}

int create_file(const std::string& filename, int flags) {
  hdfsFile file = hdfsOpenFile(hdfs_handle, filename.c_str(), O_WRONLY, 0, 0, 0);
  if (!file) {
    snprintf(errmsg, ERR_MSG_LEN, "cannot create file %s as it cannot be opened in writeonly mode", filename.c_str());
    return print_errmsg();
  }
    
  if (hdfsCloseFile(hdfs_handle, file)) {
    snprintf(errmsg, ERR_MSG_LEN, "cannot create file %s as it cannot be closed after opening", filename.c_str());
    return print_errmsg();
  } 
    
  return TILEDB_UT_OK;
}

int delete_file(const std::string& filename) {
  if (is_file(filename)) {
    if (hdfsDelete(hdfs_handle, filename.c_str(), 0) < 0) {
      snprintf(errmsg, ERR_MSG_LEN, "cannot delete file %s", filename.c_str());
      return print_errmsg();
    }
  } 
    
  return TILEDB_UT_OK;
}

int move_path(const std::string& old_path, const std::string& new_path) {
  if (!hdfsExists(hdfs_handle, new_path.c_str())) {
    snprintf(errmsg, ERR_MSG_LEN, "path %s cannot be moved to %s as it exists", old_path.c_str(), new_path.c_str());
    return print_errmsg();
  }
    
  if (hdfsRename(hdfs_handle, old_path.c_str(), new_path.c_str()) < 0) {
    snprintf(errmsg, ERR_MSG_LEN, "could not move path %s to %s", old_path.c_str(), new_path.c_str());
    return print_errmsg();
  }
    
  return TILEDB_UT_OK;
}

size_t file_size(const std::string& filename) {
  hdfsFileInfo* file_info = hdfsGetPathInfo(hdfs_handle, filename.c_str());
  if (!file_info) {
    snprintf(errmsg, ERR_MSG_LEN, "cannot get fileinfo for file %s", filename.c_str());
    print_errmsg();
    return 0;
  }

  if ((char)(file_info->mKind) != 'F') {
    snprintf(errmsg, ERR_MSG_LEN, "%s is not a file", filename.c_str());
    print_errmsg();
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
      snprintf(errmsg, ERR_MSG_LEN, "hdfs tSize width not recognized in read_from_file");
      print_errmsg();
      return 0;
  }
}

int read_from_file_kernel(hdfsFile file, void* buffer, size_t length, off_t offset) {
  if (hdfsSeek(hdfs_handle, file, (tOffset)offset) < 0) {
    snprintf(errmsg, ERR_MSG_LEN, "cannot seek to offset 0%zx in file", offset);
    return print_errmsg();
  }

  size_t max_bytes = max_tsize();
  if (max_bytes == 0) {
    return TILEDB_UT_ERR;
  }

  size_t nbytes = 0;
  char *pbuf = (char *)buffer;
  do {
    tSize bytes_read = hdfsRead(hdfs_handle, file, (void *)pbuf,  (length - nbytes) > max_bytes ? max_bytes : length - nbytes);
    if (bytes_read < 0) {
      snprintf(errmsg, ERR_MSG_LEN, "error read from file");
      return print_errmsg();
    }
    nbytes += bytes_read;
    pbuf += bytes_read;
  } while (nbytes < length);

  return TILEDB_UT_OK;
}

int read_from_file(const std::string& filename, off_t offset, void *buffer, size_t length) {
  TRACE_FN_ARG("filename:"<<std::string(filename) << " offset=" << offset << " length=" << length);
  
  hdfsFile file = hdfsOpenFile(hdfs_handle, filename.c_str(), O_RDONLY, length, 0, 0);
  if (!file) {
    snprintf(errmsg, ERR_MSG_LEN, "cannot open file %s for read", filename.c_str());
    return print_errmsg();
  }

  size_t size = file_size(filename);
  if(read_from_file_kernel(file, buffer, length>size?size:length, offset)) {
    snprintf(errmsg, ERR_MSG_LEN, "cannot read file %s", filename.c_str());
    return print_errmsg();
  }

  if (hdfsCloseFile(hdfs_handle, file)) {
    snprintf(errmsg, ERR_MSG_LEN, "cannot close file %s after read", filename.c_str());
    return print_errmsg();
  }

  return TILEDB_UT_OK;
}

int write_to_file_kernel(hdfsFile file, const void* buffer, size_t buffer_size, size_t max_bytes) {
  size_t nbytes = 0;
  char *pbuf = (char *)buffer; 
  do {
    tSize bytes_written = hdfsWrite(hdfs_handle, file, (void *)pbuf,  (buffer_size - nbytes) > max_bytes ? max_bytes : buffer_size - nbytes);
    if (bytes_written < 0) {
      snprintf(errmsg, ERR_MSG_LEN, "error write to file");
      return print_errmsg();
    }
    nbytes += bytes_written;
    pbuf += bytes_written;
  } while (nbytes < buffer_size);
    
  hdfsFlush(hdfs_handle, file);

  return TILEDB_UT_OK;
}

int append_by_replacing(hdfsFile &file, const char *filename, const void *buffer, size_t buffer_size) {
  TRACE_FN;
  // read file into read_buffer
  size_t length = file_size(std::string(filename));
  void *read_buffer = malloc(length);
  if(!read_buffer) {
    snprintf(errmsg, ERR_MSG_LEN, "Out of memory while reading file %s for appending", filename);
    return print_errmsg();
  }
  file = hdfsOpenFile(hdfs_handle, filename, O_RDONLY, length, 0, 0);
  if (!file) {
    snprintf(errmsg, ERR_MSG_LEN, "cannot open file %s for reading before appending", filename);
    return print_errmsg();
  }

  if(read_from_file_kernel(file, read_buffer, length, 0)) {
    snprintf(errmsg, ERR_MSG_LEN, "cannot read from file %s for appending", filename);
    return print_errmsg();
  }

  if (hdfsCloseFile(hdfs_handle, file)) {
    snprintf(errmsg, ERR_MSG_LEN, "cannot close file %s after read for appending", filename);
    return print_errmsg();
  }

  // TODO: is this needed or does opening in O_WRONLY automatically truncate?
  if (hdfsDelete(hdfs_handle, filename, 0)) {
    snprintf(errmsg, ERR_MSG_LEN, "cannot delete file %s after read for appending", filename);
    return print_errmsg();
  }

  // open file in write mode and append to it
  size_t max_bytes = max_tsize();
  if (max_bytes == 0) {
    return TILEDB_UT_ERR;
  }
    
  file = hdfsOpenFile(hdfs_handle, filename, O_WRONLY, max_bytes, 0, 0);
  if (!file) {
    snprintf(errmsg, ERR_MSG_LEN, "cannot open file %s for writing to append", filename);
    return print_errmsg();
  }

  if(write_to_file_kernel(file, read_buffer, length, max_bytes)) {
    snprintf(errmsg, ERR_MSG_LEN, "cannot write to file %s for replacing", filename);
    return print_errmsg();
  }
  free(read_buffer);

  // we also use this for sync - in which case, no data is actually appended
  if(buffer) {
    if(write_to_file_kernel(file, buffer, buffer_size, max_bytes)) {
      snprintf(errmsg, ERR_MSG_LEN, "cannot write to file %s for appending", filename);
      return print_errmsg();
    }
  }

  return TILEDB_UT_OK;
}

int write_to_file(const char *filename, const void *buffer, size_t buffer_size) {
  TRACE_FN_ARG("filename:"<<std::string(filename));;
  size_t max_bytes = max_tsize();
  if (max_bytes == 0) {
    return TILEDB_UT_ERR;
  }
    
  hdfsFile file = NULL;
  if (is_file(filename)) {
    // emrfs doesn't support append, so append by replacing file
    if(append_by_replacing(file, filename, buffer, buffer_size)) {
      snprintf(errmsg, ERR_MSG_LEN, "cannot append by replace for file %s", filename);
      return print_errmsg();
    }
  }
  else {
    file = hdfsOpenFile(hdfs_handle, filename, O_WRONLY, max_bytes, 0, 0);
    if (!file) {
      snprintf(errmsg, ERR_MSG_LEN, "cannot open file %s for write", filename);
      return print_errmsg();
    }

    if(write_to_file_kernel(file, buffer, buffer_size, max_bytes)) {
      snprintf(errmsg, ERR_MSG_LEN, "cannot write file %s", filename);
      return print_errmsg();
    }
  }
  
  if (hdfsCloseFile(hdfs_handle, file)) {
    snprintf(errmsg, ERR_MSG_LEN, "cannot close file %s after write", filename);
    return print_errmsg();
  }

  return TILEDB_UT_OK;    
}

int sync(const char* filename) { 
  TRACE_FN_ARG("filename:"<<std::string(filename));;
  if (!(is_file(std::string(filename)))) {
    if (hdfsExists(hdfs_handle, filename) == -1) {
      snprintf(errmsg, ERR_MSG_LEN, "sync: cannot sync %s as it does not exist", filename);
    } else if (is_dir(std::string(filename))) {
      snprintf(errmsg, ERR_MSG_LEN, "sync: no support for synch'ing path %s as it is a directory", filename);
    } else {
      snprintf(errmsg, ERR_MSG_LEN, "sync: no support for synch'ing path %s", filename);
    }
    print_errmsg();
    return TILEDB_UT_OK;
  }
    
  // emrfs doesn't support append and if we reach this point
  // the file does exist, so append by replacing file
  hdfsFile file = NULL;
  if(append_by_replacing(file, filename, NULL, 0)) {
    snprintf(errmsg, ERR_MSG_LEN, "cannot append by replace while syncing file %s", filename);
    return print_errmsg();
  }

  if (hdfsHSync(hdfs_handle, file)) {
    snprintf(errmsg, ERR_MSG_LEN, "error synch file %s", filename);
    return print_errmsg();
  }
    
  if (hdfsCloseFile(hdfs_handle, file)) {
    snprintf(errmsg, ERR_MSG_LEN, "cannot close file %s after synch", filename);
    return print_errmsg();
  }

  return TILEDB_UT_OK;        
}

int move_to_fs(const std::string& hdfs_path, const std::string& fs_path) {
  std::string command = "hdfs dfs -get " + hdfs_path + " " + fs_path;

   if (!is_file(hdfs_path)) {
    snprintf(errmsg, ERR_MSG_LEN, "%s does not exist", hdfs_path.c_str());
    return print_errmsg();
  }

  int rc = system(command.c_str());
  if (rc) {
    snprintf(errmsg, ERR_MSG_LEN, "%s could not be moved to %s", hdfs_path.c_str(), fs_path.c_str());
    return print_errmsg();
  }

  return TILEDB_UT_OK;
}
  
int move_from_fs(const std::string& fs_path, const std::string& hdfs_path) {
  std::string command = "hdfs dfs -put " + fs_path + " " + hdfs_path;

  int rc = system(command.c_str());
  if (rc) {
    snprintf(errmsg, ERR_MSG_LEN, "%s could not be moved to %s", fs_path.c_str(), hdfs_path.c_str());
    return print_errmsg();
  }

   if (!is_file(hdfs_path)) {
    snprintf(errmsg, ERR_MSG_LEN, "%s does not exist", hdfs_path.c_str());
    return print_errmsg();
  }
  
  return TILEDB_UT_OK;
}
}

#else
namespace hdfs {
int connect(const std::string& path) {
  return 0;
}

void disconnect() {
}

bool is_hdfs_path(const std::string& path) {
  if (starts_with(path, "hdfs://") || starts_with(path, "s3://") || starts_with(path, "gs://")) {
    assert(false && "No support for HDFS paths");
  }
  return false;
}

bool is_hdfs() {
  return false;
}

char *get_home_dir() {
  return NULL;
}

std::string current_dir() {
  return NULL;
}

bool is_dir(const std::string& dir) {
  return false;
}

bool is_file(const std::string& file) {
  return false;
}

std::string real_dir(const std::string& dir) {
  return NULL;
}

int create_dir(const std::string& dir) {
  return 0;
}

int delete_dir(const std::string& dir) {
  return 0;
}

std::vector<std::string> get_dirs(const std::string& dir) {
  return {};
}
    
int create_file(const std::string& filename, int flags) {
  return 0;
}

int delete_file(const std::string& filename) {
  return 0;
}

size_t file_size(const std::string& filename) {
  return 0;
}

int read_from_file(const std::string& filename, off_t offset, void *buffer, size_t length) {
  return 0;
}

int write_to_file(const char *filename, const void *buffer, size_t buffer_size) {
  return 0;
}

int move_path(const std::string& old_path, const std::string& new_path) {
  return 0;
}
    
int sync(const char* filename) {
  return 0;
}

int move_to_fs(const std::string& hdfs_path, const std::string& fs_path) {
  return 0;
}

int move_from_fs(const std::string& fs_path, const std::string& hdfs_path) {
  return 0;
}

}
#endif
