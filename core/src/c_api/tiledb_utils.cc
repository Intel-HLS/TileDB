/**
 * @file tiledb_utils.cc
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
 * Utility functions to access TileDB functionality in an opaque fashion. These 
 * convenience functions do not require TileDB_CTX handle.
 *
 */

#include "tiledb_utils.h"
#include "tiledb_storage.h"

#include <stdlib.h>
#include <cstring>
#include <fcntl.h>
#include <trace.h>

namespace TileDBUtils {

static int setup(TileDB_CTX **ptiledb_ctx, const std::string& home, const bool disable_file_locking=false)
{
  int rc;
  TileDB_Config tiledb_config;
  memset(&tiledb_config, 0, sizeof(TileDB_Config));
  tiledb_config.home_ = strdup(home.c_str());
  tiledb_config.disable_file_locking_ = disable_file_locking;
  rc = tiledb_ctx_init(ptiledb_ctx, &tiledb_config);
  free((void *)tiledb_config.home_);
  return rc;
}

static int finalize(TileDB_CTX *tiledb_ctx)
{
  return tiledb_ctx_finalize(tiledb_ctx);
}

bool is_cloud_path(const std::string& path) {
  return path.find("://") != std::string::npos;
}

/**
 * Returns 0 when workspace is created
 *        -1 when path is not a directory
 *        -2 when workspace could not be created
 *         1 when workspace exists and nothing is changed
 */
#define OK 0
#define NOT_DIR -1
#define NOT_CREATED -2
#define UNCHANGED 1
int initialize_workspace(TileDB_CTX **ptiledb_ctx, const std::string& workspace, const bool replace, const bool disable_file_locking)
{
  *ptiledb_ctx = NULL;
  int rc;
  rc = setup(ptiledb_ctx, workspace, disable_file_locking);
  if (rc) {
    return NOT_CREATED;
  }

  if (is_file(*ptiledb_ctx, workspace)) {
    return NOT_DIR;
  }

  if (is_workspace(*ptiledb_ctx, workspace)) {
    if (replace) {
      rc = tiledb_delete(*ptiledb_ctx, workspace.c_str());
      if (rc != TILEDB_OK) {	
	return NOT_CREATED;
      }
    } else {
      return UNCHANGED;
    }
  }

  rc = tiledb_workspace_create(*ptiledb_ctx, workspace.c_str());
  if (rc != TILEDB_OK) {
    rc = NOT_CREATED;
  } else {
    rc = OK;
  }

  return rc;
}

int create_workspace(const std::string &workspace, bool replace)
{
  TileDB_CTX *tiledb_ctx;
  int rc = initialize_workspace(&tiledb_ctx, workspace, replace);
  if (rc >= 0 && tiledb_ctx != NULL) {
    finalize(tiledb_ctx);
  }
  return rc;
}

bool workspace_exists(const std::string& workspace)
{
  bool exists = false;
  TileDB_CTX *tiledb_ctx;
  int rc = setup(&tiledb_ctx, workspace);
  exists = !rc && is_workspace(tiledb_ctx, workspace);
  finalize(tiledb_ctx);
  return exists;
}

bool array_exists(const std::string& workspace, const std::string& array_name)
{
  bool exists = false;
  TileDB_CTX *tiledb_ctx;
  int rc = setup(&tiledb_ctx, workspace);
  exists = !rc && is_array(tiledb_ctx, workspace + '/' + array_name);
  finalize(tiledb_ctx);
  return exists;
}

std::vector<std::string> get_array_names(const std::string& workspace)
{
  TileDB_CTX *tiledb_ctx;
  if (setup(&tiledb_ctx, workspace)) {
    return std::vector<std::string>{};
  }
  std::vector<std::string> array_names;
  std::vector<std::string> dirs = get_dirs(tiledb_ctx, workspace);
  if (!dirs.empty()) {
    for (std::vector<std::string>::iterator dir = dirs.begin() ; dir != dirs.end(); dir++) {
      std::string path(*dir);
      if (is_array(tiledb_ctx, path)) {
	size_t pos = path.find_last_of("\\/");
	if (pos == std::string::npos) {
	  array_names.push_back(path);
	} else {
	  array_names.push_back(path.substr(pos+1));
	}
      }
    }
  }
  finalize(tiledb_ctx);
  return array_names;
}

static int check_file(TileDB_CTX *tiledb_ctx, std::string filename) {
  if (is_dir(tiledb_ctx, filename)) {
    finalize(tiledb_ctx);
    snprintf(tiledb_errmsg, TILEDB_ERRMSG_MAX_LEN, "File path=%s exists as a directory\n", filename.c_str());
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

static int check_file_for_read(TileDB_CTX *tiledb_ctx, std::string filename) {
  if (check_file(tiledb_ctx, filename)) {
    return TILEDB_ERR;
  }
  if (!is_file(tiledb_ctx, filename) || file_size(tiledb_ctx, filename) == 0) {
    finalize(tiledb_ctx);
    snprintf(tiledb_errmsg, TILEDB_ERRMSG_MAX_LEN, "File path=%s does not exist or is empty\n", filename.c_str());
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

/** Allocates buffer, responsibility of caller to release buffer */
int read_entire_file(const std::string& filename, void **buffer, size_t *length)
{
  TileDB_CTX *tiledb_ctx;
  if (setup(&tiledb_ctx, parent_dir(filename)) || check_file_for_read(tiledb_ctx, filename)) {
    return TILEDB_ERR;
  }
  size_t size = file_size(tiledb_ctx, filename);
  *length = size;
  *buffer = (char *)malloc(size+1);
  if (*buffer == NULL) {
    finalize(tiledb_ctx);
    snprintf(tiledb_errmsg, TILEDB_ERRMSG_MAX_LEN, "Out-of-memory exception while allocating memory\n");
    return TILEDB_ERR;
  }
  memset(*buffer, 0, size+1);
  int rc = read_file(tiledb_ctx, filename, 0, *buffer, size);
  rc |= close_file(tiledb_ctx, filename);
  finalize(tiledb_ctx);
  return rc;
}

int read_file(const std::string& filename, off_t offset, void *buffer, size_t length)
{
  TileDB_CTX *tiledb_ctx;
  if (setup(&tiledb_ctx, parent_dir(filename)) || check_file_for_read(tiledb_ctx, filename)) {
    return TILEDB_ERR;
  }
  int rc = read_file(tiledb_ctx, filename, offset, buffer, length);
  rc |= close_file(tiledb_ctx, filename);

  finalize(tiledb_ctx);
  return rc;
}

int write_file(const std::string& filename, const void *buffer, size_t length, const bool overwrite)
{
  TileDB_CTX *tiledb_ctx;
  if (setup(&tiledb_ctx, parent_dir(filename)) || check_file(tiledb_ctx, filename)) {
    return TILEDB_ERR;
  }
  int rc = TILEDB_OK;
  if (overwrite && is_file(tiledb_ctx, filename) && delete_file(tiledb_ctx, filename)) {
    finalize(tiledb_ctx);
    snprintf(tiledb_errmsg, TILEDB_ERRMSG_MAX_LEN, "File %s exists and could not be deleted for writing\n", filename.c_str());
    return TILEDB_ERR;
  }
  rc = write_file(tiledb_ctx, filename, buffer, length);
  rc |= close_file(tiledb_ctx, filename);

  finalize(tiledb_ctx);
  return rc;
}

int delete_file(const std::string& filename)
{
  TileDB_CTX *tiledb_ctx;
  if (setup(&tiledb_ctx, parent_dir(filename)) || check_file(tiledb_ctx, filename)) {
    return TILEDB_ERR;
  }
  int rc = delete_file(tiledb_ctx, filename);
  finalize(tiledb_ctx);
  return rc;
}

int move_across_filesystems(const std::string& src, const std::string& dest)
{
  TileDB_CTX *tiledb_ctx;
  if (setup(&tiledb_ctx, parent_dir(src)) || check_file_for_read(tiledb_ctx, src)) {
    return TILEDB_ERR;
  }
  size_t size = file_size(tiledb_ctx, src);
  void *buffer = malloc(size);
  int rc = read_file(tiledb_ctx, src, 0, buffer, size);
  rc |= close_file(tiledb_ctx, src);
  finalize(tiledb_ctx);
  if (rc) {
    return TILEDB_ERR;
  }

  if (setup(&tiledb_ctx, parent_dir(dest)) || check_file(tiledb_ctx, dest)) {
    return TILEDB_ERR;
  }
  rc = write_file(tiledb_ctx, dest, buffer, size);
  rc |= close_file(tiledb_ctx, dest);
  finalize(tiledb_ctx);
  return rc;
}

int create_temp_filename(char *path, size_t path_length) {
  memset(path, 0, path_length);
  const char *tmp_dir;
  if (getenv("TMPDIR")) {
    tmp_dir = getenv("TMPDIR");
  } else {
    tmp_dir = P_tmpdir; // defined in stdio
  }
  char tmp_filename_pattern[64];
  sprintf(tmp_filename_pattern, "%s/TileDBXXXXXX", tmp_dir);
  int tmp_fd = mkstemp(tmp_filename_pattern);
  char tmp_proc_lnk[64];
  sprintf(tmp_proc_lnk, "/proc/self/fd/%d", tmp_fd);
  if (readlink(tmp_proc_lnk, path, path_length-1) < 0) {
    // Nalini-TODO
    // throw VariantStorageManagerException(std::string("Error while creating temp filename; ") + strerror(errno));
    return -1;
  }
  close(tmp_fd);
  return 0;
}

}
