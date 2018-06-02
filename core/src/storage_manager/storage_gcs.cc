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
#include "storage_posixfs.h"

#include <cerrno>
#include <cstring>
#include <clocale>
#include <iostream>
#include <memory>
#include <cstdlib>
#include <unistd.h>
#include <system_error>

#include <string>

#ifdef TILEDB_VERBOSE
#  define PRINT_ERROR(x) std::cerr << TILEDB_FS_ERRMSG << "gcs: " << x << std::endl
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif

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
  if (!buffer) {
    PRINT_ERROR("Failed to allocate buffer");
    return NULL;
  }
  memset((void *)buffer, 0, size+1);

  int rc = fs->read_from_file(filename, 0, buffer, size);
  if (rc) {
    PRINT_ERROR("Failed to read file " << filename);
  }

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

thread_local char *value = NULL;

#define GCS_PREFIX "gs://"

hdfsFS gcs_connect(struct hdfsBuilder *builder, const std::string& working_dir) {
  char *gcs_creds = getenv("GOOGLE_APPLICATION_CREDENTIALS");
  if (gcs_creds) {
    value = parse_json(gcs_creds, "project_id"); // free value after hdfsBuilderConnect as it is shallow copied.
    if (value) {
      hdfsBuilderConfSetStr(builder, "google.cloud.auth.service.account.enable", "true");
      hdfsBuilderConfSetStr(builder, "google.cloud.auth.service.account.json.keyfile", gcs_creds);
      hdfsBuilderConfSetStr(builder, "fs.gs.project.id", value);
    }
  }

  if (working_dir.empty()) {
    hdfsBuilderConfSetStr(builder, "fs.gs.working.dir", "/");
  } else {
    hdfsBuilderConfSetStr(builder, "fs.gs.working.dir", working_dir.c_str());
  }

  // Default buffer sizes are huge in the GCS connector. TileDB reads/writes in smaller chunks,
  // so the buffer size can be made a little smaller.
  hdfsBuilderConfSetStr(builder, "fs.gs.io.buffersize.write", "262144");
    
  hdfsFS hdfs_handle = hdfsBuilderConnect(builder);
  free(value);
  return hdfs_handle;
}


