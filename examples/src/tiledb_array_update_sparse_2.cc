/**
 * @file   tiledb_array_update_sparse_2.cc
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
 * It shows how to update a sparse array, including how to handle
 * deletions. Observe that this is simply a write operation.
 */

#include "c_api.h"

int main() {
  // Intialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Initialize array
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx,                                // Context 
      &tiledb_array,                             // Array object
      "my_workspace/sparse_arrays/my_array_B",   // Array name
      TILEDB_ARRAY_WRITE_UNSORTED,               // Mode
      NULL,                                      // Entire domain
      NULL,                                      // All attributes
      0);                                        // Number of attributes

  // Prepare cell buffers
  int buffer_a1[] = { 109, TILEDB_EMPTY_INT32, 108, 105 };
  size_t buffer_a2[] = { 0, 1, 2, 6 };
  const char buffer_var_a2[] = 
  { 'u', TILEDB_EMPTY_CHAR, 'v', 'v', 'v', 'v', 'y', 'y', 'y' };
  float buffer_a3[] = 
  { 
    109.1,  109.2,  TILEDB_EMPTY_FLOAT32,  TILEDB_EMPTY_FLOAT32,  
    108.1,  108.2,  105.1,  105.2 
  };
  int64_t buffer_coords[] = { 3, 2, 3, 3, 4, 1, 3, 4 };
  const void* buffers[] = 
      { buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords };
  size_t buffer_sizes[] = 
  { 
      sizeof(buffer_a1),  
      sizeof(buffer_a2),
      sizeof(buffer_var_a2),
      sizeof(buffer_a3),
      sizeof(buffer_coords)
  };

  // Write to array
  tiledb_array_write(tiledb_array, buffers, buffer_sizes); 

  // Finalize array
  tiledb_array_finalize(tiledb_array);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
