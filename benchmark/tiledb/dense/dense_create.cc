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
 */


#include "c_api.h"
#include <iostream>
#include <cstdlib>
#include <vector>
#include <sstream>
#include <tiledb_tests.h>

using namespace std;

char *tiledb_arrayname = NULL;
uint64_t dim_values[RANK] = { 0, 0 };
uint64_t chunksizes[RANK] = { 0, 0 };
int capacity = 1000000;
int enable_compression = 0;

int parse_opts(int argc, char **argv);

int main(int argc, char **argv) {

	parse_opts(argc, argv);

  /* Initialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

 /*
  * Prepare the array schema struct, initializing all numeric members to 0
  * and pointers to NULL.
  */
  TileDB_ArraySchema array_schema;

  const int attribute_num = 1;
	const char* attributes[] = { "a1" };
	const char* dimensions[] = { "X", "Y" };
	uint64_t domain[] = { 0, dim_values[0]-1, 0, dim_values[1]-1 };
	uint64_t tile_extents[] = { chunksizes[0], chunksizes[1] };
	const int types[] = { TILEDB_INT32, TILEDB_INT64 };
	int compression[RANK];
	if (enable_compression==0) {
		compression[0] = TILEDB_NO_COMPRESSION;
		compression[1] = TILEDB_NO_COMPRESSION;
	} else {
		compression[0] = TILEDB_GZIP;
		compression[1] = TILEDB_GZIP;
	}
	const int dense = 1;
	const int cell_val_num[] = { 1 };

	tiledb_array_set_schema(&array_schema, tiledb_arrayname, attributes,
			attribute_num, capacity, TILEDB_ROW_MAJOR, cell_val_num,
			compression, dense, dimensions, RANK, domain, 4*sizeof(int64_t),
			tile_extents, 2*sizeof(int64_t), TILEDB_ROW_MAJOR, types);

  /*
   * NOTE: The rest of the array schema members will be set to default values.
   * This implies that the array has "row-major" tile order, no compression,
   * and consolidation step equal to 1.
   */

  /* Create the array. */
  tiledb_array_create(tiledb_ctx, &array_schema);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}

/**
 * Parse command line parameters
 */
int parse_opts(
	int argc,
	char **argv) {

  int c;
	int help_flag = 0;
  opterr = 0;

	while ((c = getopt (argc, argv, "a:d:c:s:h")) != -1) {
		switch (c)
		{
			case 'a':	// array name
				{
					tiledb_arrayname = new char [FILENAMESIZE];
					strcpy(tiledb_arrayname, optarg);
					break;
				}
			case 'd':	// domain values
				{
					vector<uint64_t> vect;
					stringstream ss(optarg);
					int i;
					while (ss >> i)
					{
						vect.push_back(i);

						if (ss.peek() == ',')
							ss.ignore();
					}
					dim_values[0] = vect[0];
					dim_values[1] = vect[1];
					break;
				}
			case 'c': // chunk sizes
				{
					vector<uint64_t> vect;
					stringstream ss(optarg);
					int i;
					while (ss >> i)
					{
						vect.push_back(i);

						if (ss.peek() == ',')
							ss.ignore();
					}
					chunksizes[0] = vect[0];
					chunksizes[1] = vect[1];
					break;
				}
			case 's': // capacity
				{
					capacity = atoi(optarg);
					break;
				}
			case 'z':
				{
					enable_compression = 1;
					break;
				}
			case 'h': // help
				{
					help_flag = 1;
					break;
				}
			default:
				abort ();
		}
	}	// end of while

	int error = 0;
	error = (!tiledb_arrayname) ? 1 : error;
	error = (dim_values[0] == 0 && dim_values[1] == 0) ? 1 : error;
	error = (chunksizes[0] == 0 && chunksizes[1] == 0) ? 1 : error;

	if (error || help_flag == 1) {
		cout << "\n Usage: " << argv[0] << " -a arrayname -d dim0,dim1 "
				 << "-c chunksize0,chunksize1 -z (to enable compression) "
				 << "-s capacity\n";
		exit(EXIT_FAILURE);
	}
	return EXIT_SUCCESS;
}
