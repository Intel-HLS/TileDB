/**
 * @file storage_fs.h
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
 * Storage FS Base Implementation
 *
 */

#include "storage_fs.h"

#include <assert.h>

/* ****************************** */
/*        GLOBAL VARIABLES        */
/* ****************************** */

std::string tiledb_fs_errmsg = "";

StorageFS::~StorageFS() {
  // Default
}

std::string StorageFS::current_dir() {
  assert(false && "Implement in derived class");
}

bool StorageFS::is_dir(const std::string& dir) {
  assert(false && "Implement in derived class");
}

bool StorageFS::is_file(const std::string& file) {
  assert(false && "Implement in derived class");
}

std::string StorageFS::real_dir(const std::string& dir) {
  assert(false && "Implement in derived class");
}
  
int StorageFS::create_dir(const std::string& dir) {
  assert(false && "Implement in derived class");
}

int StorageFS::delete_dir(const std::string& dir) {
  assert(false && "Implement in derived class");
}

std::vector<std::string> StorageFS::get_dirs(const std::string& dir) {
  assert(false && "Implement in derived class");
}
    
std::vector<std::string> StorageFS::get_files(const std::string& dir) {
  assert(false && "Implement in derived class");
}

int StorageFS::create_file(const std::string& filename, int flags, mode_t mode) {
  assert(false && "Implement in derived class");
}

int StorageFS::delete_file(const std::string& filename) {
  assert(false && "Implement in derived class");
}

size_t StorageFS::file_size(const std::string& filename) {
  assert(false && "Implement in derived class");
}

int StorageFS::read_from_file(const std::string& filename, off_t offset, void *buffer, size_t length) {
  assert(false && "Implement in derived class");
}

int StorageFS::write_to_file(const std::string& filename, const void *buffer, size_t buffer_size) {
  assert(false && "Implement in derived class");
}

int StorageFS::move_path(const std::string& old_path, const std::string& new_path) {
  assert(false && "Implement in derived class");
}
    
int StorageFS::sync_path(const std::string& path) {
  assert(false && "Implement in derived class");
}

int StorageFS::close_file(const std::string& filename) {
  return TILEDB_FS_OK;
}

bool StorageFS::consolidation_support() {
  return false;
}
