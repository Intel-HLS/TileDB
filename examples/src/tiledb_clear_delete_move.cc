/**
 * @file   tiledb_clear_delete_move.cc
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
 * It shows how to clear, delete and move TileDB objects.
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

  // Clear an array
  tiledb_clear(tiledb_ctx, "my_workspace/sparse_arrays/my_array_B");

  // Delete a group
  tiledb_delete(tiledb_ctx, "my_workspace/dense_arrays");

  // Move a workspace
  tiledb_move(tiledb_ctx, "my_workspace", "my_workspace_2");

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
