/**
 * @file   book_keeping.h
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
 * This file defines the Buffer class that caches a file until finalization.
 */

#ifndef __BUFFER_H__
#define __BUFFER_H__

#include "tiledb_constants.h"
#include <string>

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */
  
/**@{*/
/** Return code. */
#define TILEDB_BF_OK        0
#define TILEDB_BF_ERR      -1
/**@}*/

/** Default error message. */
#define TILEDB_BF_ERRMSG std::string("[TileDB::Buffer] Error: ")

/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

/** Stores potential error messages. */
extern std::string tiledb_bf_errmsg;

class Buffer {
public:
  /* ********************************* */
  /*    CONSTRUCTORS & DESTRUCTORS     */
  /* ********************************* */

  /** Constructor */
  Buffer();

  /** Constructor
   * Set the buffer to the given buffer. In this case, it is a read-only buffer.
   * @param buffer The buffer which contains the data.
   * @param size The size of the data in the buffer.
   */
  Buffer(void *bytes, int64_t size);

  /** Destructor. */
  ~Buffer();
  
  /**
   * Get the buffer.
   */
  void *get_buffer();

  /**
   * Get the size of the filled buffer size.
   */
  int64_t get_buffer_size();

  /**
   * Reads all the data from the cached buffer into bytes.
   * @param bytes The buffer into which the data will be written.
   * @param length The size of the data to be read from the cached buffer.
   */
  int read_buffer(void *bytes, int64_t size);

  /**
   * Reads the data from the cached buffer from the offset into bytes.
   * @param offset The offset from which the data in the buffer will be read from.
   * @param bytes The buffer into which the data will be written.
   * @param length The size of the data to be read from the cached buffer.
   */
  int read_buffer(int64_t offset, void *bytes, int64_t size);

  /**
   * Appends data from bytes into the cached buffer. This operation cannot be
   * performed on read-only buffers.
   * @param bytes The buffer for the data.
   * @param length The size of the data to be written into the cached buffer.
   */
  int append_buffer(const void *bytes, int64_t size);

  /**
   * Frees the allocated cached buffer and reinitializes all associated varaibles.
   */
  void free_buffer();

protected:
  /** 
   * Sets the buffer to the given buffer. In this case, it is a read-only buffer.
   * @param buffer The buffer which contains the data.
   * @param size The size of the data in the buffer.
   */
  void set_buffer(void *bytes, int64_t size);

private:
  void *buffer = NULL;
  int64_t buffer_size = 0;
  int64_t buffer_offset = 0;
  int64_t allocated_buffer_size = 0;
  bool read_only = false;
};

#endif


