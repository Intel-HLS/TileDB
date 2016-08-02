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


#include <tiledb_tests.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <c_api.h>
#include <iostream>

using namespace std;

char *tiledb_arrayname = NULL;
int verbose = 0;
int nchunks_per_process = 0;
int toFileFlag = 0;

int parse_opts(int argc, char **argv);
void toFile(const char *filename, int *buffer, const size_t size);

int main(
	int argc,
	char **argv) {

	parse_opts(argc, argv);

	int mpi_size, mpi_rank;
	MPI_Init(&argc,&argv);
	MPI_Comm comm = MPI_COMM_WORLD;
	MPI_Comm_size(comm, &mpi_size);
	MPI_Comm_rank(comm, &mpi_rank);

	// Initialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
	TileDB_Config config;
	config.read_method_ = TILEDB_IO_MMAP;
  tiledb_ctx_init(&tiledb_ctx, &config);

	// Load array schema when the array is not initialized
  TileDB_ArraySchema array_schema;
  tiledb_array_load_schema(
      tiledb_ctx,
      tiledb_arrayname,
      &array_schema);
	uint64_t* domain = (uint64_t*) array_schema.domain_;
	uint64_t* tile_extents = (uint64_t*) array_schema.tile_extents_;	
	const uint64_t dim1 = domain[3] - domain[2] + 1;
	const uint64_t chunkdim0 = tile_extents[0];
	const uint64_t chunkdim1 = tile_extents[1];

	struct timeval g_start, g_end;
	uint64_t dim0_lo, dim0_hi, dim1_lo, dim1_hi;

	GETTIME(g_start);

	uint64_t start[2];
	/* set up dimensions of the slab this process accesses */
	for (int i = 0; i < nchunks_per_process; ++i) {
		int y = (mpi_rank*nchunks_per_process+i)%(dim1/chunkdim1);
		int x = ((mpi_rank*nchunks_per_process+i) - y)/(dim1/chunkdim1);
		dim0_lo = x * chunkdim0;
		dim0_hi = dim0_lo + chunkdim0 - 1;
		dim1_lo = y * chunkdim1;
		dim1_hi = dim1_lo + chunkdim1 - 1;
		if (i==0) {
			start[0] = dim0_lo;
			start[1] = dim1_lo;
		}
	}
	uint64_t range[] = { start[0], dim0_hi, start[1], dim1_hi };
	uint64_t readsize = sizeof(int)*(range[1]-range[0]+1)*(range[3]-range[2]+1);

	if (verbose)
		printf("Read: [%ld,%ld] and [%ld,%ld]\n", range[0],
					range[1], range[2], range[3]);

	int *buffer = (int *) malloc(readsize);

	TileDB_Array *tiledb_array;
	const char * attributes[] = { "a1" };
	void *buffers[] = { buffer };
	size_t buffersizes[] = {readsize};
	tiledb_array_init(tiledb_ctx, &tiledb_array, tiledb_arrayname,	
				TILEDB_ARRAY_READ, range, attributes, 1);
	if (tiledb_array_read(tiledb_array, buffers, buffersizes) != TILEDB_OK) {
			cout << "ERROR writing tiledb array\n";
			exit(EXIT_FAILURE);
		}
	tiledb_array_finalize(tiledb_array);

	if (toFileFlag) {
		char filename[1024];
		sprintf(filename, "./tmp/chunk_read_results_chunk%d.bin", mpi_rank);
		if (verbose) printf("writing to file: %s\n", filename);
		toFile(filename, buffer, readsize);
	}

	free(buffer);
	MPI_Finalize();

	if (mpi_rank==0) {
		GETTIME(g_end);
		if (verbose)
			printf("read time: %.3f\n", DIFF_TIME_SECS(g_start, g_end));
	}
	return EXIT_SUCCESS;
}

/**
 * Write contents of a buffer to a binary file
 */
void toFile(const char *filename, int *buffer, const size_t size) {
	int fd = open(filename, O_WRONLY | O_APPEND | O_CREAT, S_IRWXU);
	if (write(fd, (void *)buffer, size)==-1) {
		printf("File write error\n");
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

	while ((c = getopt (argc, argv, "a:n:hvf")) != -1) {
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
					nchunks_per_process = atoi(optarg);
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
	error = (nchunks_per_process==0) ? 1 : error;

	if (error || help) {
		cout << "\n Usage: " << argv[0]
				 << ":\n\n\t-a arrayname\tTileDB Array name/directory\n"
				 << "\n\t[-f]\t\tDump to file flag; Enabling it means each chunk will"
				 << "\n\t\t\tbe written as a separate binary file in $PWD/tmp/"
				 << "\n\t-n Integer\tNumber of chunks per process"
				 << "\n\t-v\t\tVerbose to print info messages\n";
	
		exit(EXIT_FAILURE);
	}
	return EXIT_SUCCESS;
}
