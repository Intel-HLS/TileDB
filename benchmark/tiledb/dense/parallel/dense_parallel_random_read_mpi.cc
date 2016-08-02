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
	
#ifndef MPI_FILE_NULL           /* MPIO may be defined in mpi.h already */
#   include <mpio.h>
#endif

using namespace std;

char *tiledb_arrayname = NULL;
char *filename = NULL;
int verbose = 0;
int ncells = 0;
int verify = 0;

int parse_opts(int argc, char **argv);

int main(
	int argc,
	char **argv) {

	int  mpi_size, mpi_rank;

	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
	MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
	MPI_Comm comm = MPI_COMM_WORLD;

	parse_opts(argc, argv);

	// Initialize context with the default configuration parameters
  TileDB_CTX* tiledb_ctx;
	TileDB_Config config = {};
	config.read_method_ = TILEDB_IO_MPI;
	config.mpi_comm_ = &comm;
  tiledb_ctx_init(&tiledb_ctx, &config);

	// Load array schema when the array is not initialized
  TileDB_ArraySchema array_schema;
  tiledb_array_load_schema(
      tiledb_ctx,
      tiledb_arrayname,
      &array_schema);
	uint64_t* domain = (uint64_t*) array_schema.domain_;
	const int dim1 = domain[3] - domain[2] + 1;	

	struct timeval start, end;

	uint64_t *buffer_dim0 = new uint64_t [ncells];
	uint64_t *buffer_dim1 = new uint64_t [ncells];
	
	int cells_per_process=ncells/mpi_size;

	GETTIME(start);
	char command[FILENAMESIZE];
	char filename_sorted[FILENAMESIZE];
	sprintf(filename_sorted,"%s_sorted",filename);
	sprintf(command, "sort -g -k1 -k2 -k3 %s > %s",filename,filename_sorted);
	system(command);

	FILE *fp = fopen(filename_sorted,"r");
  if (fp==NULL) {
    printf("file open error\n");
    exit(EXIT_FAILURE);
  }
  int id = 0;
	uint64_t r, c;
	int index=0;
  while(fscanf(fp,"%d %ld %ld",&id,&r,&c)!=EOF) {
    buffer_dim0[index] = r;
    buffer_dim1[index] = c;
    index++;
		if (index > ncells) {
			cout << "ERROR: ncells=" << ncells
				   << " does not match with number of lines"
           << " in " << filename << "\n";
			exit(EXIT_FAILURE);
		}
  }
  fclose(fp);

	TileDB_Array *tiledb_array;
	const char * attributes[] = { "a1" };
	size_t buffersizes[] = {1*sizeof(int)};
	tiledb_array_init(tiledb_ctx, &tiledb_array, tiledb_arrayname,	
				TILEDB_ARRAY_READ, NULL, attributes, 1);
	
	int x;
	int line_offset = mpi_rank*cells_per_process;
	for (int i=line_offset;i<line_offset+cells_per_process;++i) {
		r = buffer_dim0[i];
		c = buffer_dim1[i];
		if (verbose) {
			if (i==0) {
				cout << "process:: " << mpi_rank << " starting from : " << line_offset << "\n";
				cout << "process:: " << mpi_rank << " first is " << r << " " << c << "\n";
			}
		}
		void *buffers[] = {&x};
		uint64_t subarray[] = {r, r, c, c};

		tiledb_array_reset_subarray(tiledb_array, subarray);
		if (tiledb_array_read(tiledb_array, buffers, buffersizes) != TILEDB_OK) {
			cout << "ERROR writing tiledb array\n";
			exit(EXIT_FAILURE);
		}
		if (verify) {
			if (x != (int)(r*dim1+c)) {
				cout << " -- cell mismatch: " << x << "!=" << r << "*" << dim1 << "+" << c << "\n";
				MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
			}
		}
	}
	tiledb_array_finalize(tiledb_array);
	MPI_Finalize();
	GETTIME(end);
	// Finalize context
	tiledb_ctx_finalize(tiledb_ctx);

	if (verbose) {
		if (mpi_rank==0) {
			printf("read time: %.3f\n", DIFF_TIME_SECS(start, end));
		}
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

	while ((c = getopt (argc, argv, "a:f:n:vhb")) != -1) {
		switch (c)
		{
			case 'a':
				{
					tiledb_arrayname = new char [FILENAMESIZE];
					strcpy(tiledb_arrayname, optarg);
					break;
				}
			case 'b':
				{
					verify = 1;
					break;
				}
			case 'n':
				{
					ncells = atoi(optarg);
					break;
				}
			case 'v':
				{
					verbose = 1;
					break;
				}
			case 'f':
				{
					filename = new char [FILENAMESIZE];
					strcpy(filename, optarg);
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
	error = (!filename) ? 1 : error;
	error = (ncells < 1) ? 1 : error;

	if (error || help) {
		cout << "\n Usage: " << argv[0]
				 << ":\n\n\t-a arrayname\t\tTileDB Array name/directory"
				 << "\n\t-f path\t\tFile with sequence of random queries"
				 << "\n\t-n Integer\t\tNumber of cells read for all processes\n";
		exit(EXIT_FAILURE);
	}
	return EXIT_SUCCESS;
}
