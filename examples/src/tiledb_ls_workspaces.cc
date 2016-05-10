/**
 * @file   tiledb_ls_workspaces.cc
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
 * It shows how to list the TileDB workspaces.
 */

#include "c_api.h"
#include <cstdio>
#include <cstdlib>

int main() {
  // Initialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  // Initialize variables
  char* dirs[10];
  int allocated_dir_num = 10;
  int dir_num = 10;
  for(int i=0; i<dir_num; ++i)
    dirs[i] = (char*) malloc(TILEDB_NAME_MAX_LEN);

  // List workspaces
  tiledb_ls_workspaces(tiledb_ctx, (char**) dirs, &dir_num);
  for(int i=0; i<dir_num; ++i)
    printf("%s\n", dirs[i]);
 
  // Clean up
  for(int i=0; i<allocated_dir_num; ++i)
    free(dirs[i]);

  // Finalize context
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
