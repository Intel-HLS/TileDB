/**
 * @file   utils.cc
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
 * This file implements useful (global) functions.
 */

#include "tiledb_constants.h"
#include "utils.h"
#include "buffer.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <cstdio>
#include <dirent.h>
#include <fcntl.h>
#include <iostream>
#include <netdb.h>
#include <set>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>

#if defined(__APPLE__) && defined(__MACH__)
  #include <sys/types.h>
  #include <sys/sysctl.h>
  #include <net/if.h>
  #include <net/if_dl.h>
  #include <netinet/in.h>
  #include <arpa/inet.h>
#else
  #include <linux/if.h>
#endif

#include <unistd.h>
#include <zlib.h>
#include <typeinfo>

#define XSTR(s) STR(s)
#define STR(s) #s



/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifdef TILEDB_VERBOSE
#  define PRINT_ERROR(x) std::cerr << TILEDB_UT_ERRMSG << x << ".\n" 
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif




/* ****************************** */
/*        GLOBAL VARIABLES        */
/* ****************************** */

std::string tiledb_ut_errmsg = "";




/* ****************************** */
/*           FUNCTIONS            */
/* ****************************** */

bool array_read_mode(int mode) {
  return mode == TILEDB_ARRAY_READ || 
         mode == TILEDB_ARRAY_READ_SORTED_COL ||
         mode == TILEDB_ARRAY_READ_SORTED_ROW;
}

bool array_write_mode(int mode) {
  return mode == TILEDB_ARRAY_WRITE || 
         mode == TILEDB_ARRAY_WRITE_SORTED_COL || 
         mode == TILEDB_ARRAY_WRITE_SORTED_ROW || 
         mode == TILEDB_ARRAY_WRITE_UNSORTED;
}

bool array_filter_mode(int mode) {
  return mode == TILEDB_ARRAY_FILTER;
}

bool both_slashes(char a, char b) {
  return a == '/' && b == '/';
}

template<class T>
inline
bool cell_in_subarray(const T* cell, const T* subarray, int dim_num) {
  for(int i=0; i<dim_num; ++i) {
    if(cell[i] >= subarray[2*i] && cell[i] <= subarray[2*i+1])
      continue; // Inside this dimension domain

    return false; // NOT inside this dimension domain
  }
  
  return true;
}

template<class T>
int64_t cell_num_in_subarray(const T* subarray, int dim_num) {
  int64_t cell_num = 1;

  for(int i=0; i<dim_num; ++i)
    cell_num *= subarray[2*i+1] - subarray[2*i] + 1;

  return cell_num;
}

template<class T> 
int cmp_col_order(
    const T* coords_a,
    const T* coords_b,
    int dim_num) {
  for(int i=dim_num-1; i>=0; --i) {
    // a precedes b
    if(coords_a[i] < coords_b[i])
      return -1;
    // b precedes a
    else if(coords_a[i] > coords_b[i])
      return 1;
  }

  // a and b are equal
  return 0;
}

template<class T> 
int cmp_col_order(
    int64_t id_a,
    const T* coords_a,
    int64_t id_b,
    const T* coords_b,
    int dim_num) {
  // a precedes b
  if(id_a < id_b)
    return -1;

  // b precedes a
  if(id_a > id_b)
    return 1;

  // ids are equal, check the coordinates
  for(int i=dim_num-1; i>=0; --i) {
    // a precedes b
    if(coords_a[i] < coords_b[i])
      return -1;
    // b precedes a
    else if(coords_a[i] > coords_b[i])
      return 1;
  }

  // a and b are equal
  return 0;
}

template<class T> 
int cmp_row_order(
    const T* coords_a,
    const T* coords_b,
    int dim_num) {
  for(int i=0; i<dim_num; ++i) {
    // a precedes b
    if(coords_a[i] < coords_b[i])
      return -1;
    // b precedes a
    else if(coords_a[i] > coords_b[i])
      return 1;
  }

  // a and b are equal
  return 0;
}

template<class T> 
int cmp_row_order(
    int64_t id_a,
    const T* coords_a,
    int64_t id_b,
    const T* coords_b,
    int dim_num) {
  // a precedes b
  if(id_a < id_b)
    return -1;

  // b precedes a
  if(id_a > id_b)
    return 1;

  // ids are equal, check the coordinates
  for(int i=0; i<dim_num; ++i) {
    // a precedes b
    if(coords_a[i] < coords_b[i])
      return -1;
    // b precedes a
    else if(coords_a[i] > coords_b[i])
      return 1;
  }

  // a and b are equal
  return 0;
}

bool is_gcs_path(const std::string& pathURL) {
  if (!pathURL.empty() && starts_with(pathURL, "gs:")) {
#ifdef USE_HDFS
    return true;
#else
    assert(false && "GCS functionality not enabled in TileDB, build with -DUSE_HDFS=1");
#endif
  } else {
    return false;
 }
}

bool is_hdfs_path(const std::string& pathURL) {
  if (!pathURL.empty() && (starts_with(pathURL, "hdfs:") || starts_with(pathURL, "s3:"))) {
#ifdef USE_HDFS
    return true;
#else
    assert(false && "HDFS functionality not enabled in TileDB, build with -DUSE_HDFS=1");
#endif
  } else {
    return false;
  }
}

int create_dir(StorageFS *fs, const std::string& dir) {
  return fs->create_dir(dir);
}

int create_file(StorageFS *fs, const std::string& filename, int flags, mode_t mode) {
  return fs->create_file(filename, flags, mode);
}

int delete_file(StorageFS *fs, const std::string& filename) {
  return fs->delete_file(filename);
}

int create_fragment_file(StorageFS *fs, const std::string& dir) {
  // Create the special fragment file
  std::string filename = std::string(dir) + "/" + TILEDB_FRAGMENT_FILENAME;
  if (fs->create_file(filename, O_WRONLY | O_CREAT | O_SYNC,  S_IRWXU) == TILEDB_UT_ERR) {
    std::string errmsg = 
        std::string("Failed to create fragment file; ") +
        strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return TILEDB_UT_ERR;
  }

  // Success
  return TILEDB_UT_OK;
}

int delete_dir(StorageFS *fs, const std::string& dirname) {
  return fs->delete_dir(dirname);
}

int move_path(StorageFS *fs, const std::string& old_path, const std::string& new_path) {
  return fs->move_path(old_path, new_path);
}

template<class T>
bool empty_value(T value) {
  if(&typeid(T) == &typeid(int))
    return value == T(TILEDB_EMPTY_INT32);
  else if(&typeid(T) == &typeid(int64_t))
    return value == T(TILEDB_EMPTY_INT64);
  else if(&typeid(T) == &typeid(float))
    return value == T(TILEDB_EMPTY_FLOAT32);
  else if(&typeid(T) == &typeid(double))
    return value == T(TILEDB_EMPTY_FLOAT64);
  else
    return false;
}

int expand_buffer(void*& buffer, size_t& buffer_allocated_size) {
  buffer_allocated_size *= 2;
  buffer = realloc(buffer, buffer_allocated_size);
  
  if(buffer == NULL) {
    std::string errmsg = "Cannot reallocate buffer";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return TILEDB_UT_ERR;
  } else {
    return TILEDB_UT_OK;
  }
}

template<class T>
void expand_mbr(T* mbr, const T* coords, int dim_num) {
  for(int i=0; i<dim_num; ++i) {
    // Update lower bound on dimension i
    if(mbr[2*i] > coords[i])
      mbr[2*i] = coords[i];

    // Update upper bound on dimension i
    if(mbr[2*i+1] < coords[i])
      mbr[2*i+1] = coords[i];   
  }	
} 

size_t file_size(StorageFS *fs, const std::string& filename) {
  return fs->file_size(filename);
}

std::string current_dir(StorageFS *fs) {
  return fs->current_dir();
}

std::vector<std::string> get_dirs(StorageFS *fs, const std::string& dir) {
  return fs->get_dirs(dir);
}

std::vector<std::string> get_files(StorageFS *fs, const std::string& dir) {
  return fs->get_files(dir);
}

std::vector<std::string> get_fragment_dirs(StorageFS *fs, const std::string& dir) {
  std::vector<std::string> dirs = get_dirs(fs, dir);
  std::vector<std::string> fragment_dirs;
  for (auto const& dir: dirs) { 
    if (is_fragment(fs, dir)) {
      fragment_dirs.push_back(dir);
    }
  }
  return fragment_dirs;
}

#if defined(__APPLE__) && defined(__MACH__)
std::string get_mac_addr() {
  int  mib[6];
  char mac[13];
  size_t len;
  char *buf;
  unsigned char *ptr;
  struct if_msghdr *ifm;
  struct sockaddr_dl *sdl;

  mib[0] = CTL_NET;
  mib[1] = AF_ROUTE;
  mib[2] = 0;
  mib[3] = AF_LINK;
  mib[4] = NET_RT_IFLIST;
  if(((mib[5] = if_nametoindex(XSTR(TILEDB_MAC_ADDRESS_INTERFACE))) == 0) ||
     (sysctl(mib, 6, NULL, &len, NULL, 0) < 0)) {
    std::string errmsg = "Cannot get MAC address";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return "";
  }

  buf = (char*) malloc(len);
  if(sysctl(mib, 6, buf, &len, NULL, 0) < 0) {
    std::string errmsg = "Cannot get MAC address";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return "";
  }

  ifm = (struct if_msghdr *)buf;
  sdl = (struct sockaddr_dl *)(ifm + 1);
  ptr = (unsigned char *)LLADDR(sdl);
  for(int i=0; i<6; ++i)
    sprintf(mac + 2*i, "%02x", *(ptr+i));
  mac[12] ='\0';

  free(buf);
  return mac;
}
#else
std::string get_mac_addr() {
  struct ifreq s;
  int fd = socket(PF_INET, SOCK_DGRAM, IPPROTO_IP);
  char mac[13];

  strcpy(s.ifr_name, XSTR(TILEDB_MAC_ADDRESS_INTERFACE));
  if (0 == ioctl(fd, SIOCGIFHWADDR, &s)) {
    for (int i = 0; i < 6; ++i)
      sprintf(mac + 2*i, "%02x", (unsigned char) s.ifr_addr.sa_data[i]);
    mac[12] = '\0';
    close(fd);

    return mac;
  } else { // Error
    close(fd);
    std::string errmsg = "Cannot get MAC address";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return "";
  }
}
#endif

ssize_t gzip(
    unsigned char* in, 
    size_t in_size,
    unsigned char* out, 
    size_t out_size,
    const int level) {

  ssize_t ret;
  z_stream strm;
 
  // Allocate deflate state
  strm.zalloc = Z_NULL;
  strm.zfree = Z_NULL;
  strm.opaque = Z_NULL;
  ret = deflateInit(&strm, level);

  if(ret != Z_OK) {
    std::string errmsg = "Cannot compress with GZIP";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    (void)deflateEnd(&strm);
    return TILEDB_UT_ERR;
  }

  // Compress
  strm.next_in = in;
  strm.next_out = out;
  strm.avail_in = in_size;
  strm.avail_out = out_size;
  ret = deflate(&strm, Z_FINISH);

  // Clean up
  (void)deflateEnd(&strm);

  // Return 
  if(ret == Z_STREAM_ERROR || strm.avail_in != 0) {
    std::string errmsg = "Cannot compress with GZIP";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return TILEDB_UT_ERR;
  } else {
    // Return size of compressed data
    return out_size - strm.avail_out; 
  }
}

int gunzip(
    unsigned char* in, 
    size_t in_size,
    unsigned char* out, 
    size_t avail_out, 
    size_t& out_size) {
  int ret;
  z_stream strm;
  
  // Allocate deflate state
  strm.zalloc = Z_NULL;
  strm.zfree = Z_NULL;
  strm.opaque = Z_NULL;
  strm.avail_in = 0;
  strm.next_in = Z_NULL;
  ret = inflateInit(&strm);

  if(ret != Z_OK) {
    std::string errmsg = "Cannot decompress with GZIP";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return TILEDB_UT_ERR;
  }

  // Decompress
  strm.next_in = in;
  strm.next_out = out;
  strm.avail_in = in_size;
  strm.avail_out = avail_out;
  ret = inflate(&strm, Z_FINISH);

  if(ret != Z_STREAM_END) {
    std::string errmsg = "Cannot decompress with GZIP";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return TILEDB_UT_ERR;
  }

  // Clean up
  (void)inflateEnd(&strm);

  // Calculate size of compressed data
  out_size = avail_out - strm.avail_out; 

  // Success
  return TILEDB_UT_OK;
}

template<class T>
bool has_duplicates(const std::vector<T>& v) {
  std::set<T> s(v.begin(), v.end());

  return s.size() != v.size(); 
}

template<class T>
bool inside_subarray(const T* coords, const T* subarray, int dim_num) {
  for(int i=0; i<dim_num; ++i)
    if(coords[i] < subarray[2*i] || coords[i] > subarray[2*i+1])
      return false;

  return true;
}

template<class T>
bool intersect(const std::vector<T>& v1, const std::vector<T>& v2) {
  std::set<T> s1(v1.begin(), v1.end());
  std::set<T> s2(v2.begin(), v2.end());
  std::vector<T> intersect;
  std::set_intersection(s1.begin(), s1.end(),
                        s2.begin(), s2.end(),
                        std::back_inserter(intersect));

  return intersect.size() != 0; 
}

bool is_array(StorageFS *fs, const std::string& dir) {
  // Check existence
  if(is_dir(fs, dir) && 
     is_file(fs, dir + "/" + TILEDB_ARRAY_SCHEMA_FILENAME)) 
    return true;
  else
    return false;
}

template<class T>
bool is_contained(
    const T* range_A, 
    const T* range_B, 
    int dim_num) {
  for(int i=0; i<dim_num; ++i) 
    if(range_A[2*i] < range_B[2*i] || range_A[2*i+1] > range_B[2*i+1])
      return false;

  return true;
}

bool is_dir(StorageFS *fs, const std::string& dir) {
  return fs->is_dir(dir);
}

bool is_file(StorageFS *fs, const std::string& file) {
  return fs->is_file(file);
}

bool is_fragment(StorageFS *fs, const std::string& dir) {
  // Check existence
  return fs->is_file(dir + '/' + TILEDB_FRAGMENT_FILENAME);
}

bool is_group(StorageFS *fs, const std::string& dir) {
  // Check existence
  return fs->is_file(dir + '/' + TILEDB_GROUP_FILENAME);
}

bool is_metadata(StorageFS *fs, const std::string& dir) {
  // Check existence
  return fs->is_file(dir + '/' + TILEDB_METADATA_SCHEMA_FILENAME);
}

bool is_positive_integer(const char* s) {
  int i=0;

  if(s[0] == '-') // negative
    return false;

  if(s[0] == '0' && s[1] == '\0') // equal to 0
    return false; 

  if(s[0] == '+')
    i = 1; // Skip the first character if it is the + sign

  for(; s[i] != '\0'; ++i) {
    if(!isdigit(s[i]))
      return false;
  }

  return true;
}

template<class T>
bool is_unary_subarray(const T* subarray, int dim_num) {
  for(int i=0; i<dim_num; ++i) {
    if(subarray[2*i] != subarray[2*i+1])
      return false;
  }

  return true;
}

bool is_workspace(StorageFS *fs, const std::string& dir) {
  // Check existence
  return fs->is_file(dir + '/' + TILEDB_WORKSPACE_FILENAME);
}

#ifdef HAVE_MPI
int mpi_io_read_from_file(
    const MPI_Comm* mpi_comm,
    const std::string& filename,
    off_t offset,
    void* buffer,
    size_t length) {
  // Sanity check
  if(mpi_comm == NULL) {
    std::string errmsg = "Cannot read from file; Invalid MPI communicator";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return TILEDB_UT_ERR;
  }

  // Open file
  MPI_File fh;
  if(MPI_File_open(
         *mpi_comm, 
         (char*) filename.c_str(), 
         MPI_MODE_RDONLY, 
         MPI_INFO_NULL, 
         &fh)) {
    std::string errmsg = "Cannot read from file; File opening error";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return TILEDB_UT_ERR;
  }

  // Read
  MPI_File_seek(fh, offset, MPI_SEEK_SET); 
  MPI_Status mpi_status;
  if(MPI_File_read(fh, buffer, length, MPI_CHAR, &mpi_status)) {
    std::string errmsg = "Cannot read from file; File reading error";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return TILEDB_UT_ERR;
  }
  
  // Close file
  if(MPI_File_close(&fh)) {
    std::string errmsg = "Cannot read from file; File closing error";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return TILEDB_UT_ERR;
  }

  // Success
  return TILEDB_UT_OK;
}

int mpi_io_write_to_file(
    const MPI_Comm* mpi_comm,
    const char* filename,
    const void* buffer,
    size_t buffer_size) {
  // Open file
  MPI_File fh;
  if(MPI_File_open(
         *mpi_comm, 
         (char*) filename, 
         MPI_MODE_WRONLY | MPI_MODE_APPEND | 
             MPI_MODE_CREATE | MPI_MODE_SEQUENTIAL, 
         MPI_INFO_NULL, 
         &fh)) {
    std::string errmsg = 
        std::string("Cannot write to file '") + filename + 
        "'; File opening error";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return TILEDB_UT_ERR;
  }

  // Append attribute data to the file in batches of 
  // TILEDB_UT_MAX_WRITE_COUNT bytes at a time
  MPI_Status mpi_status;
  while(buffer_size > TILEDB_UT_MAX_WRITE_COUNT) {
    if(MPI_File_write(
           fh, 
           (void*) buffer, 
           TILEDB_UT_MAX_WRITE_COUNT, 
           MPI_CHAR, 
           &mpi_status)) {
      std::string errmsg = 
          std::string("Cannot write to file '") + filename + 
          "'; File writing error";
      PRINT_ERROR(errmsg);
      tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
      return TILEDB_UT_ERR;
    }
    buffer_size -= TILEDB_UT_MAX_WRITE_COUNT;
  }
  if(MPI_File_write(fh, (void*) buffer, buffer_size, MPI_CHAR, &mpi_status)) {
    std::string errmsg = 
        std::string("Cannot write to file '") + filename + 
        "'; File writing error";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return TILEDB_UT_ERR;
  }

  // Close file
  if(MPI_File_close(&fh)) {
    std::string errmsg = 
        std::string("Cannot write to file '") + filename + 
        "'; File closing error";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return TILEDB_UT_ERR;
  }

  // Success 
  return TILEDB_UT_OK;
}

int mpi_io_sync(
    StorageFS *fs,
    const MPI_Comm* mpi_comm,
    const char* filename) {
  // Open file
  MPI_File fh;
  int rc;
  if(is_dir(fs, filename))       // DIRECTORY
    rc = MPI_File_open(
             *mpi_comm, 
              (char*) filename, 
              MPI_MODE_RDONLY, 
              MPI_INFO_NULL, 
              &fh);
  else if(is_file(fs, filename))  // FILE
    rc = MPI_File_open(
             *mpi_comm, 
              (char*) filename, 
              MPI_MODE_WRONLY | MPI_MODE_APPEND | 
                  MPI_MODE_CREATE | MPI_MODE_SEQUENTIAL, 
              MPI_INFO_NULL, 
              &fh);
  else
    return TILEDB_UT_OK;     // If file does not exist, exit

  // Handle error
  if(rc) {
      std::string errmsg = 
          std::string("Cannot sync file '") + filename + 
          "'; File opening error";
      PRINT_ERROR(errmsg);
      tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
      return TILEDB_UT_ERR;
    }

  // Sync
  if(MPI_File_sync(fh)) {
    std::string errmsg = 
        std::string("Cannot sync file '") + filename + 
        "'; File syncing error";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return TILEDB_UT_ERR;
  }

  // Close file
  if(MPI_File_close(&fh)) {
    std::string errmsg = 
        std::string("Cannot sync file '") + filename + 
        "'; File closing error";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return TILEDB_UT_ERR;
  }

  // Success 
  return TILEDB_UT_OK;
}
#endif

#ifdef HAVE_OPENMP
int mutex_destroy(omp_lock_t* mtx) {
  omp_destroy_lock(mtx);

  return TILEDB_UT_OK;
}

int mutex_init(omp_lock_t* mtx) {
  omp_init_lock(mtx);

  return TILEDB_UT_OK;
}

int mutex_lock(omp_lock_t* mtx) {
  omp_set_lock(mtx);

  return TILEDB_UT_OK;
}

int mutex_unlock(omp_lock_t* mtx) {
  omp_unset_lock(mtx);

  return TILEDB_UT_OK;
}
#endif

int mutex_destroy(pthread_mutex_t* mtx) {
  if(pthread_mutex_destroy(mtx) != 0) {
    std::string errmsg = "Cannot destroy mutex";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return TILEDB_UT_ERR;
  } else {
    return TILEDB_UT_OK;
  }
}

int mutex_init(pthread_mutex_t* mtx) {
  if(pthread_mutex_init(mtx, NULL) != 0) {
    std::string errmsg = "Cannot initialize mutex";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return TILEDB_UT_ERR;
  } else {
    return TILEDB_UT_OK;
  }
}

int mutex_lock(pthread_mutex_t* mtx) {
  if(pthread_mutex_lock(mtx) != 0) {
    std::string errmsg = "Cannot lock mutex";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return TILEDB_UT_ERR;
  } else {
    return TILEDB_UT_OK;
  }
}

int mutex_unlock(pthread_mutex_t* mtx) {
  if(pthread_mutex_unlock(mtx) != 0) {
    std::string errmsg = "Cannot unlock mutex";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return TILEDB_UT_ERR;
  } else {
    return TILEDB_UT_OK;
  }
}

std::string parent_dir(StorageFS *fs, const std::string& dir) {
  // Get real dir
  std::string real_dir;
  if (fs == NULL) { // Allow fs to be NULL for support to parent_dir in tiledb_storage.h
    real_dir = dir;
  } else {
    real_dir = fs->real_dir(dir);
  }

  // Start from the end of the string
  int pos = real_dir.size() - 1;

  // Skip the potential last '/'
  if(real_dir[pos] == '/')
    --pos;

  // Scan backwords until you find the next '/'
  while(pos > 0 && real_dir[pos] != '/')
    --pos;

  return real_dir.substr(0, pos); 
}

int read_from_file(StorageFS *fs,
    const std::string& filename,
    off_t offset,
    void* buffer,
    size_t length) {
  return fs->read_from_file(filename, offset, buffer, length);
}

#define windowBits 15
#define GZIP_ENCODING 16

int read_from_file_after_decompression(StorageFS *fs, const std::string& filename, void** buffer, size_t &buffer_size, const int compression) {
  switch (compression) {
    case TILEDB_GZIP:
    case TILEDB_NO_COMPRESSION:
      break;
    default:
      std::string errmsg = std::string("Compression type not supported");
      PRINT_ERROR(errmsg);
      tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
      return TILEDB_UT_ERR;
  }
  
  size_t size = fs->file_size(filename);
  unsigned char *in = (unsigned char *)malloc(size);

  if (fs->read_from_file(filename, 0, in, size) == TILEDB_UT_ERR) {
    free(in);
    std::string errmsg = std::string("Could not read from file");
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return TILEDB_UT_ERR;
  }

  if (compression == TILEDB_NO_COMPRESSION) {
    *buffer = in;
    buffer_size = size;
    return TILEDB_UT_OK;
  }

  unsigned char out[TILEDB_GZIP_CHUNK_SIZE];

  /* allocate inflate state */
  z_stream strm = {0};
  strm.zalloc = Z_NULL;
  strm.zfree = Z_NULL;
  strm.opaque = Z_NULL;
  strm.avail_in = 0;
  strm.next_in = Z_NULL;
    
  if (inflateInit2(&strm, (windowBits + 32)) != Z_OK) {
    free(in);
    std::string errmsg = std::string("Could not inflate file");
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return TILEDB_UT_ERR;
  }

  /* decompress until deflate stream ends or end of file */
  strm.avail_in = size;
  strm.next_in = in;
  *buffer = NULL;
  buffer_size = 0;
    
  /* run inflate() on input until output buffer not full */
  int rc;
  unsigned have;
  do {
    strm.avail_out = TILEDB_GZIP_CHUNK_SIZE;
    strm.next_out = out;
    rc = inflate(&strm, Z_NO_FLUSH);
    assert(rc != Z_STREAM_ERROR);  /* state not clobbered */
    switch (rc) {
      case Z_NEED_DICT:
        rc = Z_DATA_ERROR;     /* and fall through */
      case Z_DATA_ERROR:
      case Z_MEM_ERROR:
        free(in);
        inflateEnd(&strm);
        std::string errmsg = std::string("Error encountered during inflate");
        PRINT_ERROR(errmsg);
        tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
        return TILEDB_UT_ERR;
    }

    have = TILEDB_GZIP_CHUNK_SIZE - strm.avail_out;
    *buffer =  (unsigned char *)realloc(*buffer, buffer_size+have);
    memcpy((char *)(*buffer) + buffer_size, out, have);
    buffer_size += have;
      
  } while (strm.avail_out == 0);

  /* clean up and return */
  free(in);
  inflateEnd(&strm);

  assert(rc == Z_STREAM_END); // All bytes have been decompressed

  close_file(fs, filename);
  
  return TILEDB_UT_OK;
}

std::string real_dir(StorageFS *fs, const std::string& dir) {
  return fs->real_dir(dir);
}

int64_t RLE_compress(
    const unsigned char* input,
    size_t input_size,
    unsigned char* output,
    size_t output_allocated_size,
    size_t value_size) {
  // Initializations
  int cur_run_len = 1;                           
  int max_run_len = 65535;   
  const unsigned char* input_cur = input + value_size; 
  const unsigned char* input_prev = input;   
  unsigned char* output_cur = output;  
  int64_t value_num = input_size / value_size;
  int64_t output_size = 0;
  size_t run_size = value_size + 2*sizeof(char);
  unsigned char byte;

  // Trivial case
  if(value_num == 0) 
    return 0;

  // Sanity check on input buffer
  if(input_size % value_size) {
    std::string errmsg = 
        "Failed compressing with RLE; invalid input buffer format";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg;
    return TILEDB_UT_ERR;
  }

  // Make runs
  for(int64_t i=1; i<value_num; ++i) {
    if(!memcmp(input_cur, input_prev, value_size) && 
       cur_run_len < max_run_len) {          // Expand the run
      ++cur_run_len;
    } else {                                 // Save the run
      // Sanity check on output size
      if(output_size + run_size > output_allocated_size) {
        std::string errmsg = 
            "Failed compressing with RLE; output buffer overflow";
        PRINT_ERROR(errmsg);
        tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg;
        return TILEDB_UT_ERR;
      }

      // Copy to output buffer
      memcpy(output_cur, input_prev, value_size);
      output_cur += value_size; 
      byte = (unsigned char) (cur_run_len >> 8);
      memcpy(output_cur, &byte, sizeof(char));
      output_cur += sizeof(char);
      byte = (unsigned char) (cur_run_len % 256);
      memcpy(output_cur, &byte, sizeof(char));
      output_cur += sizeof(char);
      output_size += run_size;

      // Reset current run length
      cur_run_len = 1; 
    } 

    // Update run info
    input_prev = input_cur;
    input_cur = input_prev + value_size;
  }

  // Save final run
  // --- Sanity check on size
  if(output_size + run_size > output_allocated_size) {
    std::string errmsg = 
        "Failed compressing with RLE; output buffer overflow";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg;
    return TILEDB_UT_ERR;
  }

  // --- Copy to output buffer
  memcpy(output_cur, input_prev, value_size);
  output_cur += value_size; 
  byte = (unsigned char) (cur_run_len >> 8);
  memcpy(output_cur, &byte, sizeof(char));
  output_cur += sizeof(char);
  byte = (unsigned char) (cur_run_len % 256);
  memcpy(output_cur, &byte, sizeof(char));
  output_cur += sizeof(char);
  output_size += run_size;

  // Success
  return output_size;
} 

size_t RLE_compress_bound(size_t input_size, size_t value_size) {
  // In the worst case, RLE adds two bytes per every value in the buffer.
  int64_t value_num = input_size / value_size;
  return input_size + value_num * 2;
}

size_t RLE_compress_bound_coords(
    size_t input_size, 
    size_t value_size,
    int dim_num) {
  // In the worst case, RLE adds two bytes per every value in the buffer for 
  // each of its dim_num-1 coordinates (one dimension is never compressed).
  // The last sizeof(int64_t) is to record the number of cells compressed. 
  int64_t cell_num = input_size / (dim_num*value_size); 
  return input_size + cell_num * (dim_num-1) * 2 + sizeof(int64_t);
}

int64_t RLE_compress_coords_col(
    const unsigned char* input,
    size_t input_size,
    unsigned char* output,
    size_t output_allocated_size,
    size_t value_size,
    int dim_num) {
  // Initializations
  int cur_run_len;
  int max_run_len = 65535; 
  const unsigned char* input_cur;
  const unsigned char* input_prev = input;
  unsigned char* output_cur = output; 
  size_t coords_size = value_size*dim_num;
  size_t run_size = value_size + 2*sizeof(char);
  int64_t coords_num = input_size / coords_size;
  int64_t output_size = 0;
  unsigned char byte;

  // Sanity check on input buffer format
  if(input_size % coords_size) {
    std::string errmsg = 
        "failed compressing coordinates with RLE; invalid input buffer format";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = tiledb_ut_errmsg + errmsg;
    return TILEDB_UT_ERR;
  }

  // Trivial case
  if(coords_num == 0) 
    return 0;

  // Copy the number of coordinates
  if(output_size + sizeof(int64_t) > output_allocated_size) {
    std::string errmsg = 
        "failed compressing coordinates with RLE; output buffer overflow";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = tiledb_ut_errmsg + errmsg;
    return TILEDB_UT_ERR;
  }
  memcpy(output_cur, &coords_num, sizeof(int64_t));  
  output_cur += sizeof(int64_t); 
  output_size += sizeof(int64_t);

  // Copy the first dimension intact
  // --- Sanity check on size
  if(output_size + coords_num*value_size > output_allocated_size) {
    std::string errmsg = 
        "Failed compressing coordinates with RLE; output buffer overflow";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg;
    return TILEDB_UT_ERR;
  }
  // --- Copy to output buffer
  for(int64_t i=0; i<coords_num; ++i) { 
    memcpy(output_cur, input_prev, value_size);
    input_prev += coords_size;
    output_cur += value_size; 
  }
  output_size += coords_num * value_size;

  // Make runs for each of the last (dim_num-1) dimensions
  for(int d=1; d<dim_num; ++d) {
    cur_run_len = 1;
    input_prev = input + d*value_size; 
    input_cur = input_prev + coords_size; 

    // Make single dimension run
    for(int64_t i=1; i<coords_num; ++i) {
      if(!memcmp(input_cur, input_prev, value_size) && 
         cur_run_len < max_run_len) {          // Expand the run
        ++cur_run_len;
      } else {                                       // Save the run
        // Sanity check on output size
        if(output_size + run_size > output_allocated_size) {
          std::string errmsg = 
              "Failed compressing coordinates with RLE; output buffer overflow";
          PRINT_ERROR(errmsg);
          tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg;
          return TILEDB_UT_ERR;
        }
  
        // Copy to output buffer
        memcpy(output_cur, input_prev, value_size);
        output_cur += value_size; 
        byte = (unsigned char) (cur_run_len >> 8);
        memcpy(output_cur, &byte, sizeof(char));
        output_cur += sizeof(char);
        byte = (unsigned char) (cur_run_len % 256);
        memcpy(output_cur, &byte, sizeof(char));
        output_cur += sizeof(char);
        output_size += value_size + 2*sizeof(char);

        // Update current run length
        cur_run_len = 1; 
      } 

      // Update run info
      input_prev = input_cur;
      input_cur = input_prev + coords_size;
    }

    // Save final run
    //---  Sanity check on ouput size
    if(output_size + run_size > output_allocated_size) {
      std::string errmsg = 
          "Failed compressing coordinates with RLE; output buffer overflow";
      PRINT_ERROR(errmsg);
      tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg;
      return TILEDB_UT_ERR;
    }

    // --- Copy to output buffer
    memcpy(output_cur, input_prev, value_size);
    output_cur += value_size; 
    byte = (unsigned char) (cur_run_len >> 8);
    memcpy(output_cur, &byte, sizeof(char));
    output_cur += sizeof(char);
    byte = (unsigned char) (cur_run_len % 256);
    memcpy(output_cur, &byte, sizeof(char));
    output_cur += sizeof(char);
    output_size += value_size + 2*sizeof(char);
  }

  // Success
  return output_size;
}

int64_t RLE_compress_coords_row(
    const unsigned char* input,
    size_t input_size,
    unsigned char* output,
    size_t output_allocated_size,
    size_t value_size,
    int dim_num) {
  // Initializations
  int cur_run_len;
  int max_run_len = 65535;   
  const unsigned char* input_cur;
  const unsigned char* input_prev;
  unsigned char* output_cur = output; 
  size_t coords_size = value_size*dim_num;
  int64_t coords_num = input_size / coords_size;
  int64_t output_size = 0;
  size_t run_size = value_size + 2*sizeof(char);
  unsigned char byte;

  // Sanity check on input buffer format
  if(input_size % coords_size) {
    std::string errmsg = 
        "failed compressing coordinates with RLE; invalid input buffer format";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = tiledb_ut_errmsg + errmsg;
    return TILEDB_UT_ERR;
  }

  // Trivial case
  if(coords_num == 0) 
    return 0;

  // Copy the number of coordinates
  if(output_size + sizeof(int64_t) > output_allocated_size) {
    std::string errmsg = 
        "failed compressing coordinates with RLE; output buffer overflow";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = tiledb_ut_errmsg + errmsg;
    return TILEDB_UT_ERR;
  }
  memcpy(output_cur, &coords_num, sizeof(int64_t));  
  output_cur += sizeof(int64_t); 
  output_size += sizeof(int64_t);

  // Make runs for each of the first (dim_num-1) dimensions
  for(int d=0; d<dim_num-1; ++d) {
    cur_run_len = 1;
    input_prev = input + d*value_size; 
    input_cur = input_prev + coords_size; 

    // Make single dimension run
    for(int64_t i=1; i<coords_num; ++i) {
      if(!memcmp(input_cur, input_prev, value_size) && 
         cur_run_len < max_run_len) {          // Expand the run
        ++cur_run_len;
      } else {                                 // Save the run
        // Sanity check on size
        if(output_size + run_size > output_allocated_size) {
          std::string errmsg = 
              "Failed compressing coordinates with RLE; output buffer overflow";
          PRINT_ERROR(errmsg);
          tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg;
          return TILEDB_UT_ERR;
        }
  
        // Copy to output buffer
        memcpy(output_cur, input_prev, value_size);
        output_cur += value_size; 
        byte = (unsigned char) (cur_run_len >> 8);
        memcpy(output_cur, &byte, sizeof(char));
        output_cur += sizeof(char);
        byte = (unsigned char) (cur_run_len % 256);
        memcpy(output_cur, &byte, sizeof(char));
        output_cur += sizeof(char);
        output_size += value_size + 2*sizeof(char);

        // Update current run length
        cur_run_len = 1; 
      } 

      // Update run info
      input_prev = input_cur;
      input_cur = input_prev + coords_size;
    }

    // Save final run
    // --- Sanity check on size
    if(output_size + run_size > output_allocated_size) {
      std::string errmsg = 
          "Failed compressing coordinates with RLE; output buffer overflow";
      PRINT_ERROR(errmsg);
      tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg;
      return TILEDB_UT_ERR;
    }

    // --- Copy to output buffer
    memcpy(output_cur, input_prev, value_size);
    output_cur += value_size; 
    byte = (unsigned char) (cur_run_len >> 8);
    memcpy(output_cur, &byte, sizeof(char));
    output_cur += sizeof(char);
    byte = (unsigned char) (cur_run_len % 256);
    memcpy(output_cur, &byte, sizeof(char));
    output_cur += sizeof(char);
    output_size += run_size;
  }

  // Copy the final dimension intact
  // --- Sanity check on size
  if(output_size + coords_num*value_size > output_allocated_size) {
    std::string errmsg = 
        "Failed compressing coordinates with RLE; output buffer overflow";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg;
    return TILEDB_UT_ERR;
  }
  // --- Copy to output buffer
  input_prev = input + (dim_num-1)*value_size;
  for(int64_t i=0; i<coords_num; ++i) { 
    memcpy(output_cur, input_prev, value_size);
    input_prev += coords_size;
    output_cur += value_size; 
  }
  output_size += coords_num*value_size;

  // Success
  return output_size;
}

int RLE_decompress(
    const unsigned char* input,
    size_t input_size,
    unsigned char* output,
    size_t output_allocated_size,
    size_t value_size) {
  // Initializations
  const unsigned char* input_cur = input; 
  unsigned char* output_cur = output;  
  int64_t output_size = 0;
  int64_t run_len;
  size_t run_size = value_size + 2*sizeof(char);
  int64_t run_num = input_size / run_size;
  unsigned char byte;

  // Trivial case
  if(input_size == 0) 
    return TILEDB_UT_OK;

  // Sanity check on input buffer format
  if(input_size % run_size) {
    std::string errmsg = 
        "Failed decompressing with RLE; invalid input buffer format";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg;
    return TILEDB_UT_ERR;
  }

  // Decompress runs
  for(int64_t i=0; i<run_num; ++i) {
    // Retrieve the current run length 
    memcpy(&byte, input_cur + value_size, sizeof(char));
    run_len = (((int64_t) byte) << 8);
    memcpy(&byte, input_cur + value_size + sizeof(char), sizeof(char));
    run_len += (int64_t) byte;

    // Sanity check on size
    if(output_size + value_size * run_len > output_allocated_size) {
      std::string errmsg = 
          "Failed decompressing with RLE; output buffer overflow";
      PRINT_ERROR(errmsg);
      tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg;
      return TILEDB_UT_ERR;
    }

    // Copy to output buffer
    for(int64_t j=0; j<run_len; ++j) {
      memcpy(output_cur, input_cur, value_size);
      output_cur += value_size; 
    } 

    // Update input/output tracking info
    output_size += value_size * run_len;
    input_cur += run_size;
  }

  // Success
  return TILEDB_UT_OK;
}

int RLE_decompress_coords_col(
    const unsigned char* input,
    size_t input_size,
    unsigned char* output,
    size_t output_allocated_size,
    size_t value_size,
    int dim_num) {
  // Initializations
  const unsigned char* input_cur = input; 
  unsigned char* output_cur = output;  
  int64_t input_offset = 0;
  size_t run_size = value_size + 2*sizeof(char);
  size_t coords_size = value_size * dim_num;
  int64_t run_len, coords_num;
  unsigned char byte;

  // Get the number of coordinates
  // --- Sanity check on input buffer size
  if(input_offset + sizeof(int64_t) > input_size) {
    std::string errmsg = 
        "Failed decompressing coordinates with RLE; input buffer overflow";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = tiledb_ut_errmsg + errmsg;
    return TILEDB_UT_ERR;
  }
  // --- Copy number of coordinates
  memcpy(&coords_num, input_cur, sizeof(int64_t));  
  input_cur += sizeof(int64_t); 
  input_offset += sizeof(int64_t);

  // Trivial case
  if(coords_num == 0) 
    return TILEDB_UT_OK;

  // Copy the first dimension intact
  // --- Sanity check on output buffer size
  if(coords_num * coords_size > output_allocated_size) {
    std::string errmsg = 
        "Failed decompressing coordinates with RLE; output buffer overflow";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = tiledb_ut_errmsg + errmsg;
    return TILEDB_UT_ERR;
  }
  // --- Sanity check on output buffer size
  if(input_offset + coords_num * value_size > input_size) {
    std::string errmsg = 
        "Failed decompressing coordinates with RLE; input buffer overflow";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg;
    return TILEDB_UT_ERR;
  }
  // --- Copy first dimension to output
  for(int64_t i=0; i<coords_num; ++i) { 
    memcpy(output_cur, input_cur, value_size);
    input_cur += value_size; 
    output_cur += coords_size;
  }
  input_offset += coords_num * value_size;

  // Get number of runs
  int64_t run_num = (input_size - input_offset) / run_size;

  // Sanity check on input buffer format
  if((input_size - input_offset) % run_size) {
    std::string errmsg = 
        "Failed decompressing coordinates with RLE; "
         "invalid input buffer format";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg;
    return TILEDB_UT_ERR;
  }

  // Decompress runs for each of the last (dim_num-1) dimensions
  int64_t coords_i = 0;
  int d = 1;
  output_cur = output;
  for(int64_t i=0; i<run_num; ++i) {
    // Retrieve the current run length 
    memcpy(&byte, input_cur + value_size, sizeof(char));
    run_len = (((int64_t) byte) << 8);
    memcpy(&byte, input_cur + value_size + sizeof(char), sizeof(char));
    run_len += (int64_t) byte;

    // Copy to output buffer
    for(int64_t j=0; j<run_len; ++j) {
      memcpy(
          output_cur + d*value_size + coords_i*coords_size, 
          input_cur, 
          value_size);
      ++coords_i;
    } 

    // Update input tracking info
    input_cur += run_size;
    if(coords_i == coords_num) { // Change dimension
      coords_i = 0;
      ++d;
    }
  }

  // Success
  return TILEDB_UT_OK;
}

int RLE_decompress_coords_row(
    const unsigned char* input,
    size_t input_size,
    unsigned char* output,
    size_t output_allocated_size,
    size_t value_size,
    int dim_num) {
  // Initializations
  const unsigned char* input_cur = input; 
  unsigned char* output_cur = output;  
  int64_t input_offset = 0;
  size_t run_size = value_size + 2*sizeof(char);
  size_t coords_size = value_size * dim_num;
  int64_t run_len, coords_num;
  unsigned char byte;

  // Get the number of coordinates
  // --- Sanity check on input buffer size
  if(input_offset + sizeof(int64_t) > input_size) {
    std::string errmsg = 
        "Failed decompressing coordinates with RLE; input buffer overflow";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = tiledb_ut_errmsg + errmsg;
    return TILEDB_UT_ERR;
  }
  // --- Copy number of coordinates
  memcpy(&coords_num, input_cur, sizeof(int64_t));  
  input_cur += sizeof(int64_t); 
  input_offset += sizeof(int64_t);

  // Trivial case
  if(coords_num == 0) 
    return TILEDB_UT_OK;

  // Sanity check on output buffer size
  if(coords_num * coords_size > output_allocated_size) {
    std::string errmsg = 
        "Failed decompressing coordinates with RLE; output buffer overflow";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = tiledb_ut_errmsg + errmsg;
    return TILEDB_UT_ERR;
  }

  // Get number of runs
  int64_t run_num = 
      (input_size - input_offset - coords_num * value_size) / run_size;

  // Sanity check on input buffer format
  if((input_size - input_offset - coords_num * value_size) % run_size) {
    std::string errmsg = 
        "Failed decompressing coordinates with RLE; "
         "invalid input buffer format";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg;
    return TILEDB_UT_ERR;
  }

  // Decompress runs for each of the first (dim_num-1) dimensions
  int64_t coords_i = 0;
  int d = 0;
  for(int64_t i=0; i<run_num; ++i) {
    // Retrieve the current run length 
    memcpy(&byte, input_cur + value_size, sizeof(char));
    run_len = (((int64_t) byte) << 8);
    memcpy(&byte, input_cur + value_size + sizeof(char), sizeof(char));
    run_len += (int64_t) byte;

    // Copy to output buffer
    for(int64_t j=0; j<run_len; ++j) {
      memcpy(
          output_cur + d*value_size + coords_i*coords_size, 
          input_cur, 
          value_size);
      ++coords_i;
    } 

    // Update input/output tracking info
    input_cur += run_size;
    input_offset += run_size;
    if(coords_i == coords_num) { // Change dimension
      coords_i = 0;
      ++d;
    }
  }

  // Copy the last dimension intact
  // --- Sanity check on input buffer size
  if(input_offset + coords_num * value_size > input_size) {
    std::string errmsg = 
        "Failed decompressing coordinates with RLE; input buffer overflow";
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg;
    return TILEDB_UT_ERR;
  }
  // --- Copy to output buffer
  for(int64_t i=0; i<coords_num; ++i) { 
    memcpy(
        output_cur + (dim_num-1)*value_size + i*coords_size, 
        input_cur, 
        value_size);
    input_cur += value_size; 
  }

  // Success
  return TILEDB_UT_OK;
}

bool starts_with(const std::string& value, const std::string& prefix) {
  if (prefix.size() > value.size())
    return false;
  return std::equal(prefix.begin(), prefix.end(), value.begin());
}

int sync_path(StorageFS *fs, const std::string& path) {
  return fs->sync_path(path);
}

int close_file(StorageFS *fs, const std::string& filename) {
  return fs->close_file(filename);
}

int write_to_file(
    StorageFS *fs,
    const std::string& filename,
    const void* buffer,
    size_t buffer_size) {
  return fs->write_to_file(filename, buffer, buffer_size);
}

int write_to_file_after_compression(StorageFS *fs, const std::string& filename, const void* buffer, size_t buffer_size, const int compression) {
    int rc;
  switch (compression) {
    case TILEDB_GZIP:
      break;
    case TILEDB_NO_COMPRESSION:
      rc = write_to_file(fs, filename, buffer, buffer_size);
      if (!rc ) {
        close_file(fs, filename);
      }
      return rc;
    default:
      std::string errmsg = std::string("Compression type not supported");
      PRINT_ERROR(errmsg);
      tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
      return TILEDB_UT_ERR;
  }

  unsigned have;
  z_stream strm;
  unsigned char out[TILEDB_GZIP_CHUNK_SIZE];

  /* allocate deflate state */
  strm.zalloc = Z_NULL;
  strm.zfree = Z_NULL;
  strm.opaque = Z_NULL;

  if (deflateInit2 (&strm, TILEDB_COMPRESSION_LEVEL_GZIP, Z_DEFLATED, windowBits | GZIP_ENCODING, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
      std::string errmsg = std::string("Could not initialize for gzip compression");
      PRINT_ERROR(errmsg);
      tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
      return TILEDB_UT_ERR;
  }

  /* compress until end of file */
  strm.avail_in = buffer_size;
  strm.next_in = (unsigned char *)buffer;

  /* run deflate() on input until output buffer not full, finish
     compression if all of source has been read in */
  Buffer *gzip_buffer = new Buffer();
  do {
    strm.avail_out = TILEDB_GZIP_CHUNK_SIZE;
    strm.next_out = out;
    
    if ((rc = deflate(&strm, Z_FINISH)) == Z_STREAM_ERROR) {
      deflateEnd(&strm);
      std::string errmsg = std::string("Encountered Z_STREAM_ERROR; Could not compress file");
      PRINT_ERROR(errmsg);
      tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
      return TILEDB_UT_ERR;
    }
    
    have = TILEDB_GZIP_CHUNK_SIZE - strm.avail_out;
    if (gzip_buffer->append_buffer(out, have) == TILEDB_BF_ERR) {
      deflateEnd(&strm);
      std::string errmsg = std::string("Could not write compressed bytes to internal buffer");
      PRINT_ERROR(errmsg);
      tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
      return TILEDB_UT_ERR;
    }
  } while (strm.avail_out == 0);
  
  assert(strm.avail_in == 0);     /* all input is used */
  assert(rc == Z_STREAM_END);     /* stream is complete */

  deflateEnd(&strm);

  if (write_to_file(fs, filename, gzip_buffer->get_buffer(), gzip_buffer->get_buffer_size()) == TILEDB_UT_ERR) {
    std::string errmsg = std::string("Could not write compressed bytes to internal buffer");
    PRINT_ERROR(errmsg);
    tiledb_ut_errmsg = TILEDB_UT_ERRMSG + errmsg; 
    return TILEDB_UT_ERR;
  }

  delete gzip_buffer;

  sync_path(fs, filename);
  close_file(fs, filename);

  return TILEDB_UT_OK;
}

int delete_directories(StorageFS *fs, const std::vector<std::string>& directories)
{
  // Delete old fragments
  for(auto i=0u; i<directories.size(); ++i) {
    if(fs->delete_dir(directories[i]) != TILEDB_UT_OK) {
      return TILEDB_UT_ERR;
    }
  }
  return TILEDB_UT_OK;
}

// Explicit template instantiations
template int64_t cell_num_in_subarray<int>(
    const int* subarray, 
    int dim_num);
template int64_t cell_num_in_subarray<int64_t>(
    const int64_t* subarray, 
    int dim_num);
template int64_t cell_num_in_subarray<float>(
    const float* subarray, 
    int dim_num);
template int64_t cell_num_in_subarray<double>(
    const double* subarray, 
    int dim_num);

template bool cell_in_subarray<int>(
    const int* cell,
    const int* subarray,
    int dim_num);
template bool cell_in_subarray<int64_t>(
    const int64_t* cell,
    const int64_t* subarray,
    int dim_num);
template bool cell_in_subarray<float>(
    const float* cell,
    const float* subarray,
    int dim_num);
template bool cell_in_subarray<double>(
    const double* cell,
    const double* subarray,
    int dim_num);

template int cmp_col_order<int>(
    const int* coords_a,
    const int* coords_b,
    int dim_num);
template int cmp_col_order<int64_t>(
    const int64_t* coords_a,
    const int64_t* coords_b,
    int dim_num);
template int cmp_col_order<float>(
    const float* coords_a,
    const float* coords_b,
    int dim_num);
template int cmp_col_order<double>(
    const double* coords_a,
    const double* coords_b,
    int dim_num);

template int cmp_col_order<int>(
    int64_t id_a,
    const int* coords_a,
    int64_t id_b,
    const int* coords_b,
    int dim_num);
template int cmp_col_order<int64_t>(
    int64_t id_a,
    const int64_t* coords_a,
    int64_t id_b,
    const int64_t* coords_b,
    int dim_num);
template int cmp_col_order<float>(
    int64_t id_a,
    const float* coords_a,
    int64_t id_b,
    const float* coords_b,
    int dim_num);
template int cmp_col_order<double>(
    int64_t id_a,
    const double* coords_a,
    int64_t id_b,
    const double* coords_b,
    int dim_num);

template int cmp_row_order<int>(
    const int* coords_a,
    const int* coords_b,
    int dim_num);
template int cmp_row_order<int64_t>(
    const int64_t* coords_a,
    const int64_t* coords_b,
    int dim_num);
template int cmp_row_order<float>(
    const float* coords_a,
    const float* coords_b,
    int dim_num);
template int cmp_row_order<double>(
    const double* coords_a,
    const double* coords_b,
    int dim_num);

template int cmp_row_order<int>(
    int64_t id_a,
    const int* coords_a,
    int64_t id_b,
    const int* coords_b,
    int dim_num);
template int cmp_row_order<int64_t>(
    int64_t id_a,
    const int64_t* coords_a,
    int64_t id_b,
    const int64_t* coords_b,
    int dim_num);
template int cmp_row_order<float>(
    int64_t id_a,
    const float* coords_a,
    int64_t id_b,
    const float* coords_b,
    int dim_num);
template int cmp_row_order<double>(
    int64_t id_a,
    const double* coords_a,
    int64_t id_b,
    const double* coords_b,
    int dim_num);

template bool empty_value<int>(int value);
template bool empty_value<int64_t>(int64_t value);
template bool empty_value<float>(float value);
template bool empty_value<double>(double value);

template void expand_mbr<int>(
    int* mbr, 
    const int* coords, 
    int dim_num);
template void expand_mbr<int64_t>(
    int64_t* mbr, 
    const int64_t* coords, 
    int dim_num);
template void expand_mbr<float>(
    float* mbr, 
    const float* coords, 
    int dim_num);
template void expand_mbr<double>(
    double* mbr, 
    const double* coords, 
    int dim_num);

template bool has_duplicates<std::string>(const std::vector<std::string>& v);

template bool inside_subarray<int>(
    const int* coords, 
    const int* subarray, 
    int dim_num);
template bool inside_subarray<int64_t>(
    const int64_t* coords, 
    const int64_t* subarray, 
    int dim_num);
template bool inside_subarray<float>(
    const float* coords, 
    const float* subarray, 
    int dim_num);
template bool inside_subarray<double>(
    const double* coords, 
    const double* subarray, 
    int dim_num);

template bool intersect<std::string>(
    const std::vector<std::string>& v1,
    const std::vector<std::string>& v2);

template bool is_contained<int>(
    const int* range_A, 
    const int* range_B, 
    int dim_num);
template bool is_contained<int64_t>(
    const int64_t* range_A, 
    const int64_t* range_B, 
    int dim_num);
template bool is_contained<float>(
    const float* range_A, 
    const float* range_B, 
    int dim_num);
template bool is_contained<double>(
    const double* range_A, 
    const double* range_B, 
    int dim_num);

template bool is_unary_subarray<int>(const int* subarray, int dim_num);
template bool is_unary_subarray<int64_t>(const int64_t* subarray, int dim_num);
template bool is_unary_subarray<float>(const float* subarray, int dim_num);
template bool is_unary_subarray<double>(const double* subarray, int dim_num);

