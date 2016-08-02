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

/**
 * Structure to pass
 * parameters to threads
 */
struct mystruct {
	int64_t range[2*RANK];
	int id;
	char arrayname[1024];
	TileDB_CTX *tiledb_ctx;
	int chunkdim0, chunkdim1;
	int dim0, dim1;
	int **chunks;
	int buffer_size;
	int *block_ids;
	int nblocks;
	int chunkcount;
};

char *tiledb_arrayname = NULL;
char *datadir = NULL;
int verbose = 0;
int nthreads = 0;

int parse_opts(int argc, char **argv);
void *pthread_write(void *args);
void *whole_array_write(void *args);

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

	int blockcount = (dim0/chunkdim0)*(dim1/chunkdim1);

	if (verbose) {
		cout << "Blockcount: " << blockcount << "\n";
		cout << "Number of threads: " << nthreads << "\n";
	}

	pthread_t threads[blockcount];

	// Read chunks to local buffers
	int *chunks[blockcount];
	char filename[FILENAMESIZE];
	int buffer_size = chunkdim0 * chunkdim1 * sizeof(int);

	long elements = 0;
	for (int i = 0; i < blockcount; ++i) {
		sprintf(filename, "%s/chunk%d.bin", datadir, i);
		if (verbose)
			cout << "Reading file: " << filename << "...";
		chunks[i] = new int [chunkdim0 * chunkdim1];
		ifstream ifile(filename, ios::in|ios::binary);
		ifile.seekg(0, ios::beg);
		if (ifile.is_open()) {
			ifile.read((char*)chunks[i], buffer_size);
		} else cout << "Unable to open file: " << filename << "\n";
		ifile.close();
		elements += buffer_size / sizeof(int);
		if (verbose)
			cout << elements << " elements read completed\n";
	}

	struct mystruct thread_data;
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

	struct timeval start, end;

	strcpy(thread_data.arrayname, tiledb_arrayname);
	thread_data.tiledb_ctx = tiledb_ctx;
	thread_data.chunkdim0 = chunkdim0;
	thread_data.chunkdim1 = chunkdim1;
	thread_data.dim0 = dim0;
	thread_data.dim1 = dim1;
	thread_data.chunkcount = blockcount;
	thread_data.buffer_size = chunkdim0*chunkdim1*sizeof(int);
	int blocks_per_thread = blockcount/nthreads;
	thread_data.block_ids = new int [blocks_per_thread];
	thread_data.nblocks=blocks_per_thread;
	thread_data.chunks = chunks;

	const int TWOGB = 2*1000*1000*1000;
	if (nthreads==1 && dim0*dim1*sizeof(int) <= TWOGB) {
		if (dim0*dim1*sizeof(int) > TWOGB) {
			cout << "Run chunk by chunk\n";
			exit(EXIT_FAILURE);
		}
		whole_array_write((void*)&thread_data);
		// Finalize context
		tiledb_ctx_finalize(tiledb_ctx);
		return EXIT_SUCCESS;
	}

	GETTIME(start);
	for (int i = 0; i < nthreads; ++i) {
		for (int j = 0; j < blocks_per_thread; j++) {
			if (j==0) {
				thread_data.block_ids[0] = i*blocks_per_thread;
			} else {
				thread_data.block_ids[j] = thread_data.block_ids[j-1] + 1;
			}
		}
		thread_data.id = i;

    pthread_create(&threads[i], NULL, pthread_write, &thread_data);
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
 * Entry point of the thread
 * Note that the TileDB array init must be
 * called here
 */
void *pthread_write(void *args) {

	struct mystruct *writeArgs = (struct mystruct *) args;

	struct timeval start, end;
	int dim0_lo, dim0_hi, dim1_lo, dim1_hi;
	int chunkdim0 = writeArgs->chunkdim0;
	int chunkdim1 = writeArgs->chunkdim1;
	uint64_t dim1 = writeArgs->dim1;
	int64_t range[4];
	int **chunks = (int **) writeArgs->chunks;

	for(int i = 0; i < writeArgs->nblocks; ++i) {
		int y = writeArgs->block_ids[i]%(dim1/chunkdim1);
		int x = (writeArgs->block_ids[i] - y)/(dim1/chunkdim1);
		dim0_lo = x * chunkdim0;
		dim0_hi = dim0_lo + chunkdim0 - 1;
		dim1_lo = y * chunkdim1;
		dim1_hi = dim1_lo + chunkdim1 - 1;
		range[0] = dim0_lo;
		range[2] = dim1_lo;
		range[1] = dim0_hi;
		range[3] = dim1_hi;

		TileDB_Array *tiledb_array;
		// Initialize array
		tiledb_array_init(writeArgs->tiledb_ctx, &tiledb_array,
				writeArgs->arrayname, TILEDB_ARRAY_WRITE, range, NULL, 0); 
		const void * buffers[] = {chunks[writeArgs->block_ids[i]]};
		size_t buffer_sizes[] = {(size_t)writeArgs->buffer_size};
		// Write to array
		if (tiledb_array_write(tiledb_array, buffers, buffer_sizes) != TILEDB_OK) {
			cout << "ERROR writing tiledb array\n";
			pthread_exit(NULL);
		}
		// Finalize array
		GETTIME(start);
		tiledb_array_finalize(tiledb_array);
		system("sync");
		GETTIME(end);
		if (verbose) {
			printf("finalize time: %.3f\n", DIFF_TIME_SECS(start, end));
		}
	}
	return NULL;	
}

/**
 * In case the array is less then 2GB in size,
 * write the entire by calling tiledb_array_write
 * once
 */
void *whole_array_write(void *args) {
	struct mystruct *writeArgs = (struct mystruct *) args;
	struct timeval start, end;
	int dim0 = writeArgs->dim0;
	int dim1 = writeArgs->dim1;
	int chunkdim0 = writeArgs->chunkdim0;
	int chunkdim1 = writeArgs->chunkdim1;
	int64_t range[4];
	size_t buffer_size = dim0*dim1*sizeof(int);
	int *buffer = new int [buffer_size];
	for (int i = 0; i < writeArgs->nblocks; ++i) {
		memcpy((buffer+(i*chunkdim0*chunkdim1)),
				writeArgs->chunks[i], chunkdim0*chunkdim1*sizeof(int));
	}

	range[0] = 0;
	range[1] = dim0 - 1;
	range[2] = 0;
	range[3] = dim1 - 1;

	GETTIME(start);
	TileDB_Array *tiledb_array;
	// Initialize array
	tiledb_array_init(writeArgs->tiledb_ctx, &tiledb_array,
			writeArgs->arrayname, TILEDB_ARRAY_WRITE, range, NULL, 0);
	const void * buffers[] = {buffer};
	size_t buffer_sizes[] = {(size_t)buffer_size};
	// Write to array
	if (tiledb_array_write(tiledb_array, buffers, buffer_sizes) != TILEDB_OK) {
		cout << "ERROR writing tiledb array\n";
		pthread_exit(NULL);
	}
	GETTIME(end);
	float w = DIFF_TIME_SECS(start, end);
	// Finalize array
	GETTIME(start);
	tiledb_array_finalize(tiledb_array);
	system("sync");
	GETTIME(end);
	float f = DIFF_TIME_SECS(start, end);
	if (verbose)
		printf("whole array write time: %.3f\n", w+f);
	free(buffer);
	return NULL;
}

/**
 * Parse command line parameters
 */
int parse_opts(
	int argc,
	char **argv) {

  int c, help = 0;
  opterr = 0;

	while ((c = getopt (argc, argv, "a:f:t:hv")) != -1) {
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
			case 't':
				{
					nthreads = atoi(optarg);
					break;
				}
			case 'v':
				{
					verbose = 1;
					break;
				}
			case 'h':
				{
					help = 1;
					break;
				}
			default:
				abort ();
		}
	}	// end of while

	int error = 0;
	error = (!tiledb_arrayname) ? 1 : error;
	error = (!datadir) ? 1 : error;
	error = (nthreads < 1) ? 1 : error;

	if (error || help) {
		cout << "\n Usage: " << argv[0]
				 << ":\n\n\t-a arrayname\t\tTileDB Array name/directory"
				 << "\n\t-f path\t\tDirectory containing binaries of the chunk"
				 << "\n\t-t Integer\tNumber of threads"
				 << "\n\t-v\t\tVerbose to print info messages\n";
	
		exit(EXIT_FAILURE);
	}
	return EXIT_SUCCESS;
}
