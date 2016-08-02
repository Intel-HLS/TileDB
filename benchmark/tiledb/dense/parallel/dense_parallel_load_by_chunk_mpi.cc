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
#include <mpi.h>
#include <fcntl.h>
	
#ifndef MPI_FILE_NULL           /* MPIO may be defined in mpi.h already */
#   include <mpio.h>
#endif

using namespace std;

char *tiledb_arrayname = NULL;
char *datadir = NULL;
int verbose = 0;
int nthreads = 0;

int parse_opts(int argc, char **argv);

int main(
	int argc,
	char **argv) {

	parse_opts(argc, argv);

	// Initialize context with the default configuration parameters
	TileDB_CTX* tiledb_ctx;
	TileDB_Config config = {};
	config.read_method_ = TILEDB_IO_MMAP;
	tiledb_ctx_init(&tiledb_ctx, &config);

	MPI_Init(&argc, &argv);
	int mpi_rank = MPI::COMM_WORLD.Get_rank();
	int mpi_size = MPI::COMM_WORLD.Get_size();
	MPI_Comm comm  = MPI_COMM_WORLD;

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
	size_t chunk_byte_size = chunkdim0 * chunkdim1 * sizeof(int);
	int nchunks_per_process = blockcount/mpi_size;

	if (mpi_rank==0) {
		printf("Chunks/proc=%d\n", nchunks_per_process);
	}

	MPI_Barrier(MPI_COMM_WORLD);
	struct timeval g_start, g_end;
	//GETTIME(g_start);

	// Initialize data buffer 
	struct timeval start, end;
	GETTIME(start);
	int **chunks = (int **) malloc(sizeof(int*)*nchunks_per_process);
	char filename[1024];
	int elements = 0;
	int chunkid = 0;
	for (int i=0;i<nchunks_per_process;++i) {
		chunkid = mpi_rank*nchunks_per_process+i;
		chunks[i] = (int *) malloc(chunk_byte_size);
		sprintf(filename, "%s/chunk%d.bin", datadir, chunkid);
		if (verbose) {
			printf("Reading file: %s...", filename);
		}
		int fd = open(filename, O_RDONLY);
		if (fd != -1) {
			int bytes = read(fd, (void*)chunks[i], chunk_byte_size);
			if (bytes==0) {
				cout << "ERROR: 0 bytes read from: " << filename << "\n";
				exit(EXIT_FAILURE);
			}
		} else {
			printf("Unable to open file: %s\n", filename);
			MPI_Abort(comm, EXIT_FAILURE);
		}
		close(fd);
		elements += chunk_byte_size / sizeof(int);
		if (verbose)
			printf("%d elements read completed\n", elements);
	}
	GETTIME(end);

	if (verbose)
		printf("read time: %.3f\n", DIFF_TIME_SECS(start,end));

	int64_t dim0_lo, dim0_hi, dim1_lo, dim1_hi;
	int64_t range[4];
	
	GETTIME(g_start);

	for (int i=0;i<nchunks_per_process;++i) {

		int64_t y = (mpi_rank*nchunks_per_process+i)%(dim1/chunkdim1);
		int64_t x = ((mpi_rank*nchunks_per_process+i) - y)/(dim1/chunkdim1);
		if (i==0) {
			dim0_lo = x * chunkdim0;
			dim1_lo = y * chunkdim1;
		}
		dim0_hi = dim0_lo + chunkdim0 - 1;
		dim1_hi = dim1_lo + chunkdim1 - 1;
	}

	range[0] = dim0_lo;
	range[1] = dim0_hi;
	range[2] = dim1_lo;
	range[3] = dim1_hi;

	TileDB_Array *tiledb_array;
	tiledb_array_init(tiledb_ctx, &tiledb_array, tiledb_arrayname,	
			TILEDB_ARRAY_WRITE, range, NULL, 0);

	const void* buffers[] = {chunks};
	size_t buffer_sizes[] = {chunk_byte_size};
	if (tiledb_array_write(tiledb_array, buffers, buffer_sizes) != TILEDB_OK) {
		cerr << "TileDB write error\n";
		MPI_Abort(comm, EXIT_FAILURE);
	}
	
	tiledb_array_finalize(tiledb_array);

	MPI_Finalize();
	system("sync");
	GETTIME(g_end);

	// Finalize context
	tiledb_ctx_finalize(tiledb_ctx);

	if (mpi_rank==0) {
		if (verbose)
			printf("%.3f\n", DIFF_TIME_SECS(g_start, g_end));
	}
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

	while ((c = getopt (argc, argv, "a:f:hv")) != -1) {
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
				 << ":\n\n\t-a arrayname\tTileDB Array name/directory"
				 << "\n\t-f path\t\tDirectory containing binaries of the chunk"
				 << "\n\t-v\t\tVerbose to print info messages\n";
	
		exit(EXIT_FAILURE);
	}
	return EXIT_SUCCESS;
}
