/**
 * @file   hdfs.cc
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
 * HDFS Support for StorageFS
 */

#include "storage_hdfs.h"

#include <assert.h>

#ifdef USE_HDFS

#include "hdfs.h"
#include "url.h"
#include "utils.h"

#include "trace.h"

#include <iostream>
#include <memory>

#ifdef TILEDB_VERBOSE
#  define PRINT_ERROR(x) std::cerr << TILEDB_FS_ERRMSG << "hdfs: " << x << std::endl
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif

HDFS::HDFS(const std::string& home) {
  struct hdfsBuilder *builder = hdfsNewBuilder();
  if (!builder) {
    assert(false && "Error getting new hdfs builder instance");
  }

  hdfsBuilderSetForceNewInstance(builder);

  url path_url(home);
  std::string name_node;
  int16_t nport = 0;
  assert((!path_url.protocol().compare("hdfs") || !path_url.protocol().compare("s3") || !path_url.protocol().compare("gs"))
         && "Home URL not supported");

  if (path_url.host().empty()) {
    if (!path_url.port().empty()) {
      PRINT_ERROR(std::string("home=") + home + " not supported. hdfs host and port have to be both empty");
      assert(false && "Home URL not supported: hdfs host and port have to be both empty");
    }
    name_node.assign("default");
  } else if(path_url.protocol().compare("hdfs") != 0) { // s3/gs protocols
    name_node.assign(path_url.protocol() + "://" + path_url.host());
  } else {
    if (path_url.port().empty()) {
      PRINT_ERROR(std::string("home=") + home + "not supported.hdfs host and port have to be specified together");
      assert(false && "Home URL not supported: hdfs host and port have to be specified together");
    }
    name_node.assign(path_url.host());
    nport = path_url.nport();
  }
  
  hdfsBuilderSetNameNode(builder, name_node.c_str());
  if (!path_url.port().empty()) {
    hdfsBuilderSetNameNodePort(builder, nport);
  }
  
  hdfs_handle = hdfsBuilderConnect(builder);
  if (!hdfs_handle) {
    assert(false && "Error with hdfs builder connection");
  }

  if (path_url.path().empty()) {
    hdfsSetWorkingDirectory(hdfs_handle, "/");
  } else {
    hdfsSetWorkingDirectory(hdfs_handle, home.c_str());
  }
  hdfsGetWorkingDirectory(hdfs_handle, home_dir, TILEDB_NAME_MAX_LEN);
}

HDFS::~HDFS() {
  if (hdfsDisconnect(hdfs_handle)) {
    PRINT_ERROR("disconnect error");
  }
  hdfs_handle = NULL;
}

static int print_errmsg(const std::string& errmsg) {
  if (errmsg.length() > 0) {
    PRINT_ERROR(errmsg);
    tiledb_fs_errmsg = TILEDB_FS_ERRMSG + errmsg;
  }
  return TILEDB_FS_ERR;
}

std::string HDFS::current_dir() {
  char working_dir[TILEDB_NAME_MAX_LEN];
  if (hdfsGetWorkingDirectory(hdfs_handle, working_dir, TILEDB_NAME_MAX_LEN) == NULL) {
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

  
bool HDFS::is_dir(const std::string& dir) {
  return is_path(hdfs_handle, dir.c_str(), 'D');
}

bool HDFS::is_file(const std::string& file) {
  return is_path(hdfs_handle, file.c_str(), 'F');
}

std::string HDFS::real_dir(const std::string& dir) {
  if (dir.empty()) {
    return current_dir();
  } else if (is_hdfs_path(dir)) {
    // absolute path
    return dir;
  } else if (starts_with(dir, "/")) {
    // seems to be an absolute path but without protocol/host information.
    print_errmsg(dir + ": Not a valid HDFS path");
    assert(false && "Not a valid HDFS path");
  } else {
    // relative path
    return current_dir() + "/" + dir;
  }
}
  
int HDFS::create_dir(const std::string& dir) {
  if (is_dir(dir)) {
    return print_errmsg(std::string("Cannot create directory ") + dir + "; Directory already exists");
  }

  if (hdfsCreateDirectory(hdfs_handle, dir.c_str()) < 0) {
    return print_errmsg(std::string("Cannot create directory ") + dir);
  }
    
  return TILEDB_FS_OK;
}

int HDFS::delete_dir(const std::string& dir) {
  if (is_dir(dir)) {
    if (hdfsDelete(hdfs_handle, dir.c_str(), 1) < 0) {
      return print_errmsg(std::string("Cannot delete directory ") + dir);
    }
  }else {
    return print_errmsg(std::string("Cannot delete path at ") + dir);
  }
  
  return TILEDB_FS_OK;
}

std::vector<std::string> HDFS::get_dirs(const std::string& dir) {
  std::vector<std::string> path_list;

  int num_entries = 0;
  hdfsFileInfo *file_info = hdfsListDirectory(hdfs_handle, dir.c_str(), &num_entries);
  if (!file_info) {
    print_errmsg(std::string("Cannot list contents of dir ") + dir);
  } else {
    for (int i=0; i<num_entries; i++) {
      if (file_info[i].mKind == 'D') {
        path_list.push_back(std::string(file_info[i].mName));
      }
    }
  }

  return path_list;
}
    
int HDFS::create_file(const std::string& filename, int flags, mode_t mode) {
  hdfsFile file = hdfsOpenFile(hdfs_handle, filename.c_str(), O_WRONLY, 0, 0, 0);
  if (!file) {
    return print_errmsg(std::string("Cannot create file ") + filename + "; Open error");
  }
    
  if (hdfsCloseFile(hdfs_handle, file)) {
    return print_errmsg(std::string("Cannot create file ") + filename + "; Close error");
  }
  
  return TILEDB_FS_OK;
}

int HDFS::delete_file(const std::string& filename) {
  if (is_file(filename)) {
    if (hdfsDelete(hdfs_handle, filename.c_str(), 0) < 0) {
      return print_errmsg(std::string("Cannot delete file ") + filename);
    }
  } else {
    return print_errmsg(std::string("Cannot delete file") + filename + " that is not a file");
  }
  
  return TILEDB_FS_OK;
}

size_t HDFS::file_size(const std::string& filename) {
  hdfsFileInfo* file_info = hdfsGetPathInfo(hdfs_handle, filename.c_str());
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
      return print_errmsg(std::string("Error reading file"));
    }
    nbytes += bytes_read;
    pbuf += bytes_read;
  } while (nbytes < length);

  return TILEDB_FS_OK;
}

int HDFS::read_from_file(const std::string& filename, off_t offset, void *buffer, size_t length) {
    TRACE_FN_ARG("filename:"<<std::string(filename) << " offset=" << offset << " length=" << length);

  // Workaround for error messages of the type -
  // readDirect: FSDataInputStream#read error:
  // java.lang.UnsupportedOperationException: Byte-buffer read unsupported by input stream
  //	at org.apache.hadoop.fs.FSDataInputStream.read(FSDataInputStream.java:146)

  int old_fd, new_fd;
  fflush(stderr);
  old_fd = dup(2);
  new_fd = open("/dev/null", O_WRONLY);
  dup2(new_fd, 2);
  close(new_fd);
  
  hdfsFile file = hdfsOpenFile(hdfs_handle, filename.c_str(), O_RDONLY, length, 0, 0);

  fflush(stderr);
  dup2(old_fd, 2);
  close(old_fd);

  if (!file) {
    std::string errmsg = std::string("Cannot open file ") + filename + " for read";
    return print_errmsg(errmsg);
  }

  int rc = TILEDB_FS_OK;;
  size_t size = file_size(filename);
  if(read_from_file_kernel(hdfs_handle, file, buffer, length>size?size:length, offset)) {
    rc = print_errmsg(std::string("Cannot read file ") + filename);
  }

  if (hdfsCloseFile(hdfs_handle, file)) {
    rc = print_errmsg(std::string("Cannot close file ") + filename + " after read");
  }

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
    return print_errmsg(std::string("Error flushing file"));
  }

  return TILEDB_FS_OK;
}

int HDFS::write_to_file(const std::string& filename, const void *buffer, size_t buffer_size) {
  TRACE_FN_ARG("filename:"<<std::string(filename));;
  size_t max_bytes = max_tsize();
  if (max_bytes == 0) {
    return TILEDB_FS_ERR;
  }

  int rc = TILEDB_FS_OK;
  hdfsFile file = NULL;
  if (is_file(filename)) {
    assert(false && "TBD: No support for appending to existing file");
    /*    // emrfs doesn't support append, so append by replacing file
    if(append_by_replacing(file, filename, buffer, buffer_size)) {
      std::string errmsg = std::string("Cannot append by replace for file") + filename;
      rc = print_errmsg(errmsg);
      } */
  }
  else {
    file = hdfsOpenFile(hdfs_handle, filename.c_str(), O_WRONLY, max_bytes, 0, 0);
    if (!file) {
      return print_errmsg(std::string("Cannot open file " + filename + " for write"));
    }

    if(write_to_file_kernel(hdfs_handle,file, buffer, buffer_size, max_bytes)) {
      rc = print_errmsg( std::string("Error writing to file " + filename));
    }
  }

  if (rc == TILEDB_FS_OK && hdfsHSync(hdfs_handle, file)) {
    rc= print_errmsg(std::string("Cannot synch file " + filename + " after write"));
  }
  
  if (hdfsCloseFile(hdfs_handle, file)) {
    return print_errmsg(std::string("Cannot close file " + filename + " after write"));
  }

  return rc;
}

int HDFS::move_path(const std::string& old_path, const std::string& new_path) {  
  if (!hdfsExists(hdfs_handle, new_path.c_str())) {
    return print_errmsg(std::string("Cannot move path ") + old_path + " to " + new_path + " as it exists");
  }
    
  if (hdfsRename(hdfs_handle, old_path.c_str(), new_path.c_str()) < 0) {
    return print_errmsg(std::string("Cannot rename path ") + old_path + " to " + new_path);
  }

  return TILEDB_FS_OK;
}
    
int HDFS::sync(const std::string& path) {
  // Files are synched up as soon as they are written. So this is a no-op.
  return TILEDB_FS_OK;
}

bool HDFS::consolidation_support() {
  print_errmsg("TBD: No consolidation locking support for distributed file systems.");
  return false;
}

#endif
