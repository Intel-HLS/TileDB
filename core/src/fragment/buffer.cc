/**
 * @file   book_keeping.cc
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
 * This file implements the Buffer class.
 */

#include "buffer.h"

#include <assert.h>
#include <iostream>
#include <string>
#include <string.h>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifdef TILEDB_VERBOSE
#  define PRINT_ERROR(x) std::cerr << TILEDB_BF_ERRMSG << x << ".\n" 
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif

/* ****************************** */
/*        GLOBAL VARIABLES        */
/* ****************************** */

std::string tiledb_bf_errmsg = "";

Buffer::Buffer() {
}

Buffer::Buffer(void *bytes, int64_t size) {
  set_buffer(bytes, size);
}

Buffer::~Buffer() {
  free_buffer();
}

void *Buffer::get_buffer() {
  return this->buffer;
}

int64_t Buffer::get_buffer_size() {
  return buffer_size;
}

void Buffer::set_buffer(void *bytes, int64_t size) {
  this->buffer = bytes;
  this->buffer_size = size;
  this->allocated_buffer_size = size;
  this->buffer_offset = 0;
  this->read_only = true;
}

int Buffer::read_buffer(void *bytes, int64_t size) {
  assert(bytes != NULL);
  assert(buffer != NULL);
  assert(size > 0);
  
  if (buffer_offset + size > buffer_size) {
    free_buffer();
    std::string errmsg = "Cannot read from buffer; End of buffer reached";
    PRINT_ERROR(errmsg);
    tiledb_bf_errmsg = TILEDB_BF_ERRMSG + errmsg;
    return TILEDB_BF_ERR;  
  }

  void *pmem = memcpy(bytes, (char *)buffer+buffer_offset, size);
  assert(pmem == bytes);

  buffer_offset += size;
  
  return TILEDB_BF_OK;
}

int Buffer::read_buffer(int64_t offset, void *bytes, int64_t size) {
  assert(bytes != NULL);
  assert(buffer != NULL);
  assert(size >= 0);

  if (size == 0) {
    return TILEDB_BF_OK;
  }
  
  if (offset + size > buffer_size) {
    free_buffer();
    std::string errmsg = "Cannot read from buffer; End of buffer reached";
    PRINT_ERROR(errmsg);
    tiledb_bf_errmsg = TILEDB_BF_ERRMSG + errmsg;
    return TILEDB_BF_ERR;  
  }

  void *pmem = memcpy(bytes, (char *)buffer+offset, size);
  assert(pmem == bytes);

  buffer_offset = offset + size;
  
  return TILEDB_BF_OK;
}

#define CHUNK 1024
int Buffer::append_buffer(const void *bytes, int64_t size) {
  assert(read_only == false);
  assert(bytes != NULL);
  assert(size > 0);
  
  if (buffer == NULL || buffer_size+size > buffer_size) {
    int64_t alloc_size = allocated_buffer_size + ((size/CHUNK)+1)*CHUNK;
    buffer = realloc(buffer, alloc_size);
    if (buffer == NULL) {
      free_buffer();
      std::string errmsg =
          "Cannot write to buffer; Mem allocation error";
      PRINT_ERROR(errmsg);
      tiledb_bf_errmsg = TILEDB_BF_ERRMSG + errmsg;
      return TILEDB_BF_ERR;
    }

    allocated_buffer_size = alloc_size;
  }

  void *pmem = memcpy((char *)buffer+buffer_size, bytes, size);
  assert(pmem == (char *)buffer+buffer_size);
  
  buffer_size += size;

  return TILEDB_BF_OK;
}

void Buffer::free_buffer() {
  free(buffer);
  buffer = NULL;
  buffer_offset = 0;
  buffer_size = 0;
  allocated_buffer_size = 0;
}

