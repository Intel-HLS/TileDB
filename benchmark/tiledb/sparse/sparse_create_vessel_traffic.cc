/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 MIT and Intel Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 * Demonstrates how to create the AIS vessel traffic sparse array
 */

#include "c_api.h"
#include <iostream>
#include <cstdlib>

using namespace std;

int main(int argc, char **argv) {
  /* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

	if (argc < 2) {
		cout << "Usage: " << argv[0] << " arrayname\n";
		exit(EXIT_FAILURE);
	}

 /*
  * Prepare the array schema struct, initalizing all numeric members to 0
  * and pointers to NULL.
  */
  TileDB_ArraySchema array_schema;
	const int attribute_num = 7;
  const char* attributes[] = { "SOG", "COG", "Heading", "ROT", "Status", "VoyageID", "MMSI" };
	const char* dimensions[] = { "X", "Y" };
  const int64_t domain[] = { 0, 359999999, 0, 179999999};
  const int types[] = { TILEDB_INT64, TILEDB_INT64, TILEDB_INT64, TILEDB_INT64, TILEDB_INT64, TILEDB_INT64,
													TILEDB_INT64, TILEDB_INT64 };
	//const int compression[] =
	//{ TILEDB_GZIP, TILEDB_GZIP, TILEDB_GZIP, TILEDB_GZIP,
	//	TILEDB_GZIP, TILEDB_GZIP, TILEDB_GZIP, TILEDB_GZIP };
	const int compression[] =
	{ TILEDB_NO_COMPRESSION, TILEDB_NO_COMPRESSION, TILEDB_NO_COMPRESSION, TILEDB_NO_COMPRESSION,
		TILEDB_NO_COMPRESSION, TILEDB_NO_COMPRESSION, TILEDB_NO_COMPRESSION, TILEDB_NO_COMPRESSION };
	const int dense = 0;
	const int RANK = 2;
	const int cell_val_num[] = { 1, 1, 1, 1, 1, 1, 1 };
	// ensure same extents between TileDB and SciDB. Our performance should not vary with this
	int64_t tile_extents[] = { 10000, 10000 };
	int capacity = 10000;

	tiledb_array_set_schema(&array_schema, argv[1], attributes,
			attribute_num, capacity, TILEDB_ROW_MAJOR, cell_val_num,
			compression, dense, dimensions, RANK, domain, 4*sizeof(int64_t),
			tile_extents, 2*sizeof(int64_t), TILEDB_ROW_MAJOR, types);
			//NULL, 0, TILEDB_ROW_MAJOR, types);

  /*
   * NOTE: The rest of the array schema members will be set to default values.
   * This implies that the array is sparse, has irregular tiles,
   * and consolidation step equal to 1.
   */

  /* Create the array. */
  tiledb_array_create(tiledb_ctx, &array_schema);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
