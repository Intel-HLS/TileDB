/**
 * @file   tiledb_array_write_dense_2.cc
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
 * It shows how to write to a dense array invoking the write function
 * multiple times. This will have the same effect as program
 * tiledb_array_write_dense_1.cc.
 */

#include "tiledb.h"

int main(int argc, char *argv[]) {
  // Initialize context with home dir if specified in command line, else
  // initialize with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  if (argc > 1) {
    TileDB_Config tiledb_config;
    tiledb_config.home_ = argv[1];
    tiledb_ctx_init(&tiledb_ctx, &tiledb_config);
  } else {
    tiledb_ctx_init(&tiledb_ctx, NULL);
  }

  // Initialize array
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx,                                // Context 
      &tiledb_array,                             // Array object
      "my_workspace/dense_arrays/my_array_A",    // Array name
      TILEDB_ARRAY_WRITE,                        // Mode
      NULL,                                      // Entire domain
      NULL,                                      // All attributes
      0);                                        // Number of attributes

  // Prepare cell buffers - #1
  int buffer_a1[] = { 0,  1,  2,  3, 4,  5 };
  size_t buffer_a2[] = { 0,  1,  3,  6, 10, 11, 13, 16 };
  const char buffer_var_a2[] = "abbcccddddeffggghhhh";
  float* buffer_a3 = NULL; 
  const void* buffers[] = { buffer_a1, buffer_a2, buffer_var_a2, buffer_a3 };
  size_t buffer_sizes[] = 
  { 
      6*sizeof(int),          // 6 cels on a1 
      8*sizeof(size_t), 20,   // 8 cells on a2 
      0                       // no cells on a3
  };

  // Write to array - #1
  tiledb_array_write(tiledb_array, buffers, buffer_sizes); 

  // Prepare cell buffers - #2
  int buffer_a1_2[] = { 6,  7, 8,  9,  10, 11, 12, 13, 14, 15 };
  size_t buffer_a2_2[] = { 0,  1,  3,  6, 10, 11, 13, 16 };
  const char buffer_var_a2_2[] = "ijjkkkllllmnnooopppp";
  float buffer_a3_2[] = 
  {
      0.1,  0.2,  1.1,  1.2,  2.1,  2.2,  3.1,  3.2,     // Upper left tile
      4.1,  4.2,  5.1,  5.2,  6.1,  6.2,  7.1,  7.2,     // Upper right tile
      8.1,  8.2,  9.1,  9.2,  10.1, 10.2, 11.1, 11.2,    // Lower left tile
      12.1, 12.2, 13.1, 13.2, 14.1, 14.2, 15.1, 15.2,    // Lower right tile
  };
  const void* buffers_2[] = {
       buffer_a1_2, buffer_a2_2, buffer_var_a2_2, buffer_a3_2 };
  size_t buffer_sizes_2[] = 
  { 
      10*sizeof(int),          // 6 cels on a1 
      8*sizeof(size_t), 20,    // 8 cells on a2 
      32*sizeof(float)         // 16 cells on a3 (2 values each)
  };

  // Write to array - #2
  tiledb_array_write(tiledb_array, buffers_2, buffer_sizes_2); 

  // Finalize array
  tiledb_array_finalize(tiledb_array);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
