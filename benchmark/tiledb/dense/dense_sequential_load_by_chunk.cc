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
#include <fstream>
#include <string>
#include <cstring>
#include <vector>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <tiledb_tests.h>

using namespace std;

char *tiledb_arrayname = NULL;
char *datadir = NULL;
int verbose = 0;
int coreid = 0;
int enable_affinity = 0;

int parse_opts(int argc, char **argv);
void affinitize(int coreid);

int main(
	int argc,
	char **argv) {

	parse_opts(argc, argv);

	if (enable_affinity)
		affinitize(coreid);

	struct timeval start, end;

	// Initialize context with the default configuration parameters
	TileDB_CTX* tiledb_ctx;
	tiledb_ctx_init(&tiledb_ctx, NULL);

	GETTIME(start);
	// Initialize array
	TileDB_Array* tiledb_array;
	if (tiledb_array_init(
			tiledb_ctx,
			&tiledb_array,
			tiledb_arrayname,
			TILEDB_ARRAY_WRITE,
			NULL,
			NULL,
			0) != TILEDB_OK) {
		cout << "ERROR: TileDB array cannot be initialized\n";
		exit(EXIT_FAILURE);
	}
	GETTIME(end);
	float inittime = DIFF_TIME_SECS(start, end);

	// Load array schema when the array is not initialized
  TileDB_ArraySchema array_schema;
  tiledb_array_load_schema(
      tiledb_ctx,
      tiledb_arrayname,
      &array_schema);
	uint64_t* domain = (uint64_t*) array_schema.domain_;
	uint64_t* tile_extents = (uint64_t*) array_schema.tile_extents_;

	const int dim0 = domain[1] - domain[0] + 1;
	const int dim1 = domain[3] - domain[2] + 1;;
	const int chunkdim0 = tile_extents[0];
	const int chunkdim1 = tile_extents[1];
	int blockcount = (dim0/chunkdim0)*(dim1/chunkdim1);

	// Free array schema
  tiledb_array_free_schema(&array_schema);

	if (verbose) {
		cout << "Blockcount: " << blockcount << "\n";
	}

	// Read chunks to local buffers
	int *chunks[blockcount];
	char filename[1024];
	int buffer_size = chunkdim0 * chunkdim1 * sizeof(int);

	GETTIME(start);
	for (int i = 0; i < blockcount; ++i) {
		if (verbose) {
			sprintf(filename, "%s/chunk%d.bin", datadir, i);
			cout << "Reading file: " << filename << "...";
		}
		chunks[i] = new int [chunkdim0 * chunkdim1];
		int64_t x = i/(dim1/chunkdim1) * chunkdim0;
		int64_t y = i % (dim1/chunkdim1) * chunkdim1; 
		for (int j=0;j<chunkdim0;++j) {
			for (int k=0;k<chunkdim1;++k) {
				chunks[i][j*chunkdim1+k] = (x+j)*dim1+(y+k);
			}
		}
		if (verbose) {
			cout << "done\n";
		}
	}
	GETTIME(end);
	if (verbose) {
		printf("read time: %.3f\n",DIFF_TIME_SECS(start,end));
	}

	size_t buffer_sizes[] = {(size_t)buffer_size};

	GETTIME(start);
	for (int i = 0; i < blockcount; ++i) {
		const void * buffers[] = {chunks[i]};
		// Write to array
		tiledb_array_write(tiledb_array, buffers, buffer_sizes); 
	}
	// Finalize array
	tiledb_array_finalize(tiledb_array);
	system("sync");
	GETTIME(end);

	// Finalize context
	tiledb_ctx_finalize(tiledb_ctx);

	cout << "write time: " << inittime + DIFF_TIME_SECS(start,end) << "\n";
	return EXIT_SUCCESS;
}

/**
 * Parse command line parameters
 */
int parse_opts(
	int argc,
	char **argv) {

  int c, help = 0;
  opterr = 0;

	while ((c = getopt (argc, argv, "a:f:u:hv")) != -1) {
		switch (c)
		{
			case 'a':
				{
					tiledb_arrayname = new char [FILENAMESIZE];
					strcpy(tiledb_arrayname, optarg);
					break;
				}
		case 'f':
				{
					datadir = new char [FILENAMESIZE];
					strcpy(datadir, optarg);
					break;
				}	
			case 'u':
				{
					enable_affinity=1;
					coreid = atoi(optarg);
					break;
				}
			case 'h':
				{
					help = 1;
					break;
				}
			case 'v':
				{
					verbose = 1;
					break;
				}
			default:
				abort ();
		}
	}	// end of while

	int error = 0;
	error = (!tiledb_arrayname) ? 1 : error;
	error = (!datadir) ? 1 : error;

	if (error || help) {
		cout << "\n Usage: " << argv[0] << ":\n\n\t-a arrayname\t\tTileDB Array name/directory\n"
				<< "\n\t-f path\t\tDirectory containing the binary chunk files"
				<< "\n\t[-u coreid]\t\tOptional core id to affinitize this process\n";
	
		exit(EXIT_FAILURE);
	}
	return EXIT_SUCCESS;
}

/**
 * Affinitize the current process to a given CPU
 */
void affinitize(
	int coreid) {
#ifdef __linux__
	cpu_set_t mask;
	int status;

	CPU_ZERO(&mask);
	CPU_SET(coreid, &mask);
	status = sched_setaffinity(0, sizeof(mask), &mask);
	if (status != 0)
	{
		cerr << "sched setaffinity error\n";
		exit(EXIT_FAILURE);
	}
#endif
}

