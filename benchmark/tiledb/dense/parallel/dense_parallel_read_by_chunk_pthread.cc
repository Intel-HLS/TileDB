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
#include <map>
#include <sstream>
#include <fcntl.h>

using namespace std;

/**
 * Struct containing thread parameters
 */
struct mystruct {
	int id;
	char arrayname[1024];
	TileDB_CTX *tiledb_ctx;
	int ncells;
	int range[2*RANK];
	int toFile;
	int sum;
	int chunkdim0, chunkdim1;
};

char *tiledb_arrayname = NULL;
int nthreads = 0;
int toFileFlag = 0;
int verbose = 0;


int parse_opts(int argc, char **argv);
void *pthread_read(void *);
void toFile(const char *, int *, const int , const int );

int main(
	int argc,
	char **argv) {

	parse_opts(argc, argv);

	// Initialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

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

	if (verbose) {
		cout << "running with: " << dim0 << "," << dim1
				 << "," << chunkdim0 << "," << chunkdim1
				 << "," << nthreads << "," << toFileFlag << "\n";
	}

	struct mystruct thread_data[nthreads];
	int dim0_lo, dim0_hi, dim1_lo, dim1_hi;
	int ncells_per_thread = chunkdim0*chunkdim1;

	pthread_t threads[nthreads];
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

	struct timeval start, end;

	GETTIME(start);
	for (int i = 0; i < nthreads; ++i) {
		thread_data[i].id = i;
		thread_data[i].tiledb_ctx = tiledb_ctx;
		thread_data[i].chunkdim0 = chunkdim0;
		thread_data[i].chunkdim1 = chunkdim1;

		int y = i%(dim1/chunkdim1);
		int x = (i - y)/(dim1/chunkdim1);
		dim0_lo = x * chunkdim0;
		dim0_hi = dim0_lo + chunkdim0 - 1;
		dim1_lo = y * chunkdim1;
		dim1_hi = dim1_lo + chunkdim1 - 1;
		if (verbose) {
			cout << "range for thread " << i << "::"; 
			cout << dim0_lo << ",";
			cout << dim0_hi << ",";
			cout << dim1_lo << ",";
			cout << dim1_hi << "\n";
		}
		thread_data[i].range[0] = dim0_lo;
		thread_data[i].range[1] = dim0_hi;
		thread_data[i].range[2] = dim1_lo;
		thread_data[i].range[3] = dim1_hi;
		thread_data[i].ncells = ncells_per_thread;
		thread_data[i].toFile = toFileFlag;
		
		strcpy(thread_data[i].arrayname, tiledb_arrayname);
		thread_data[i].sum = 0;

		pthread_create(&threads[i], &attr, pthread_read, &thread_data[i]);
	}
	for (int i = 0; i < nthreads; ++i) {
		pthread_join(threads[i], NULL);
	}
	GETTIME(end);

	// Finalize context
	tiledb_ctx_finalize(tiledb_ctx);

	if (verbose) {
		printf("write time: %.3f\n", DIFF_TIME_SECS(start, end));
	}
	return EXIT_SUCCESS;
}

/**
 * Entry point of thread fn
 * It must initialize TileDB array
 * and this method reads a given
 * subarray by chunks
 */
void *pthread_read(void *args) {

	struct mystruct *readArgs = (struct mystruct *) args;

	struct timeval start, end;
	int *buffer = new int [readArgs->ncells];
	float time = 0.0;

	TileDB_Array *tiledb_array;
	const char * attributes[] = { "a1" };
	int64_t subarray[] = {readArgs->range[0], readArgs->range[1],
		readArgs->range[2], readArgs->range[3]};
	GETTIME(start);
	// Initialize array
	tiledb_array_init(readArgs->tiledb_ctx, &tiledb_array, readArgs->arrayname,	
			TILEDB_ARRAY_READ, subarray, attributes, 1);

	void *buffers[] = {buffer};
	size_t buffersizes[] = {readArgs->ncells*sizeof(int)};

	// Read from array
	if (tiledb_array_read(tiledb_array, buffers, buffersizes) != TILEDB_OK) {
		cout << "ERROR writing tiledb array\n";
		pthread_exit(NULL);
	}

	tiledb_array_finalize(tiledb_array);
	GETTIME(end);

	time += DIFF_TIME_SECS(start,end);
	
	if (readArgs->toFile) {
		char filename[1024];
		sprintf(filename, "./tmp/chunk_read_results_chunk%d.bin", readArgs->id);
		if (verbose) cout << "writing to file: " << filename << "\n";
		toFile(filename, buffer, readArgs->chunkdim0, readArgs->chunkdim1);
	}	

	return NULL;
}

void toFile(
	const char *filename,
	int *buffer,
	const int readDim1,
	const int readDim2) {

	int fd = open(filename, O_WRONLY | O_CREAT, S_IRWXU);
	size_t size = readDim1*readDim2*sizeof(int);
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

	while ((c = getopt (argc, argv, "a:t:hvf")) != -1) {
		switch (c)
		{
			case 'a':
				{
					tiledb_arrayname = new char [FILENAMESIZE];
					strcpy(tiledb_arrayname, optarg);
					break;
				}
			case 't':
				{
					nthreads = atoi(optarg);
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
	error = (nthreads==0) ? 1 : error;

	if (error || help) {
		cout << "\n Usage: " << argv[0]
				 << ":\n\n\t-a arrayname\tTileDB Array name/directory\n"
				 << "\n\t[-f]\t\tDump to file flag; Enabling it means each chunk will"
				 << "\n\t\t\tbe written as a separate binary file in $PWD/tmp/"
				 << "\n\t-t Integer\tNumber of threads"
				 << "\n\t-v\t\tVerbose to print info messages\n";
	
		exit(EXIT_FAILURE);
	}
	return EXIT_SUCCESS;
}
