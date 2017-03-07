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
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <time.h>
#include <unistd.h>
#include <vector>
#include <tiledb_tests.h>
#include <sys/time.h>

using namespace std;

char *tiledb_arrayname = NULL;
int verbose = 0;
int coreid = 0;
int enable_affinity = 0;
int nchunks = 0;
int toFileFlag = 0;

int parse_opts(int argc, char **argv);
void toFile(const char *, int *, const size_t);
void affinitize(int coreid);

int main(
	int argc,
	char **argv) {

	parse_opts(argc, argv);

	if (enable_affinity) {
		affinitize(coreid);
	}

  TileDB_CTX* tiledb_ctx;
	TileDB_Config config = {};
	config.read_method_ = TILEDB_IO_MMAP;
  tiledb_ctx_init(&tiledb_ctx, &config);
  //tiledb_ctx_init(&tiledb_ctx, NULL);

	// Load array schema when the array is not initialized
  TileDB_ArraySchema array_schema;
  tiledb_array_load_schema(
      tiledb_ctx,
      tiledb_arrayname,
      &array_schema);
	uint64_t* domain = (uint64_t*) array_schema.domain_;
	uint64_t* tile_extents = (uint64_t*) array_schema.tile_extents_;	
	const int dim0 = domain[1] - domain[0] + 1;
	const int dim1 = domain[3] - domain[2] + 1;
	const int chunkdim0 = tile_extents[0];
	const int chunkdim1 = tile_extents[1];
	
	// Free array schema
  tiledb_array_free_schema(&array_schema);

	if (verbose) {
		cout << "Running with: " << dim0 << "," << dim1 << "," << chunkdim0	
				 << "," << chunkdim1 << "," << nchunks << "," << toFileFlag << "\n";
	}

	int dim0_lo, dim0_hi, dim1_lo, dim1_hi;

	struct timeval start, end;
	int64_t subarray[4];
	subarray[0] = 0;
	subarray[2] = 0;

	for (int i = 0; i < nchunks; ++i) {
		int y = i%(dim1/chunkdim1);
		int x = (i - y)/(dim1/chunkdim1);
		dim0_lo = x * chunkdim0;
		dim0_hi = dim0_lo + chunkdim0 - 1;
		dim1_lo = y * chunkdim1;
		dim1_hi = dim1_lo + chunkdim1 - 1;
		subarray[1] = dim0_hi;
		subarray[3] = dim1_hi;
	}

	if (verbose) {
		cout << "Reading range: [" << subarray[0] << "," <<
			subarray[1] << "," <<
			subarray[2] << "," <<
			subarray[3] << "]\n";
	}

	GETTIME(start);
	size_t buffer_size = (subarray[1] - subarray[0] + 1) * (subarray[3] - subarray[2] + 1);
	int *buffer = new int [buffer_size];
	TileDB_Array *tiledb_array;
	const char * attributes[] = { "a1" };
	// Initialize array
	if (tiledb_array_init(tiledb_ctx, &tiledb_array, tiledb_arrayname,	
			TILEDB_ARRAY_READ, subarray, attributes, 1)!= TILEDB_OK) {
		cout << "ERROR: Cannot initialize TileDB array\n";
		exit(EXIT_FAILURE);
	}

	void *buffers[] = {buffer};
	size_t buffersizes[] = {buffer_size*sizeof(int)};

	// Read from array
	if (tiledb_array_read(tiledb_array, buffers, buffersizes) != TILEDB_OK) {
		cout << "ERROR writing tiledb array\n";
		pthread_exit(NULL);
	}

	tiledb_array_finalize(tiledb_array);
	GETTIME(end);

	// Finalize context
	tiledb_ctx_finalize(tiledb_ctx);

	if (toFileFlag) {
		char filename[1024];
		if (verbose) {
			sprintf(filename, "./tmp/chunk_read_results_chunk%d.bin", 0);
			cout << "writing to file: " << filename << "\n";
		}
		toFile(filename, buffer, buffer_size*sizeof(int));
	}

	if (verbose) {
		printf("read time: %.3f secs\n", DIFF_TIME_SECS(start, end));
	}
	return EXIT_SUCCESS;
}

/**
 * Func to write the contents of a given buffer
 * to a binary file
 */
void toFile(const char *filename, int *buffer, const size_t size) {
	int fd = open(filename, O_WRONLY | O_CREAT, S_IRWXU);
	if (write(fd, (void *)buffer, size)==-1) {
		cout << "File write error\n";
		exit(EXIT_FAILURE);
	}
	close(fd);
}

/**
 * Parse command line parameters
 */
int parse_opts(
	int argc,
	char **argv) {

  int c, help = 0;
  opterr = 0;

	while ((c = getopt (argc, argv, "a:n:u:hvf")) != -1) {
		switch (c)
		{
			case 'a':
				{
					tiledb_arrayname = new char [FILENAMESIZE];
					strcpy(tiledb_arrayname, optarg);
					break;
				}
			case 'n':
				{
					nchunks = atoi(optarg);
					break;
				}	
			case 'u':
				{
					enable_affinity=1;
					coreid = atoi(optarg);
					break;
				}
			case 'f':
				{
					toFileFlag = atoi(optarg);
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
	error = (nchunks==0) ? 1 : error;

	if (error || help) {
		cout << "\n Usage: " << argv[0]
				 << ":\n\n\t-a arrayname\tTileDB Array name/directory\n"
				 << "\n\t[-f]\t\tDump to file flag; Enabling it means each chunk will"
				 << "\n\t\t\tbe written as a separate binary file in $PWD/tmp/"
				 << "\n\t-n Integer\tNumber of chunks to read sequentially"
				 << "\n\t-v\t\tVerbose to print info messages"
				 << "\n\t[-u coreid]\tOptional core id to affinitize this process\n";
	
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

