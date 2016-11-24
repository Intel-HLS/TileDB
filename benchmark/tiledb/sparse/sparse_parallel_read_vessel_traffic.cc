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
 * Reads the AIS vessel traffic data in parallel. Check the schema of the table
 * defined in sparse_create_vessel_traffic.cc. In this script, we read only the
 * X,Y coordinates, i.e. scaled lat-longs. To read more data from TileDB,
 * add the attributes in the "attributes" buffer and also modify buffer_sizes
 * data structure accordingly. Further details on how to populate these buffers
 * can be found in core TileDB documentation
 */

#include <iostream>
#include <fstream>
#include <tiledb_tests.h>
#include <fcntl.h>
#include "c_api.h"

using namespace std;

int main(int argc, char **argv) {

	int mpi_rank = 0;
	int mpi_size = 0;
	MPI_Init(&argc,&argv);
	MPI_Comm comm = MPI_COMM_WORLD;
	MPI_Comm_size(comm, &mpi_size);
	MPI_Comm_rank(comm, &mpi_rank);

	if (argc < 9) {
		cout << "Usage: " << argv[0] << " arrayname dim0_lo dim0_hi dim1_lo "
         << "dim1_hi n toFile nqueries\n";
		exit(EXIT_FAILURE);
	}

	/* Initialize context with the default configuration parameters. */
	TileDB_CTX* tiledb_ctx;
	TileDB_Config config = {};
	config.read_method_ = TILEDB_IO_MMAP;
	config.mpi_comm_ = &comm;
	tiledb_ctx_init(&tiledb_ctx, &config);

	char arrayname[10240];
	strcpy(arrayname, argv[1]);
	const uint64_t dim0_lo = atoll(argv[2]);
	const uint64_t dim0_hi = atoll(argv[3]);
	const uint64_t dim1_lo = atoll(argv[4]);
	const uint64_t dim1_hi = atoll(argv[5]);
	const int readsize = atoi(argv[6]);
	const int toFileFlag = atoi(argv[7]);
	const int nqueries  = atoi(argv[8]);

	/* Subset over attribute "a1" and the coordinates. */
	const char* attributes[] = { TILEDB_COORDS };

	struct timeval start, end, g_start, g_end;

	GETTIME(g_start);
	/* Initialize the array in READ mode. */
	TileDB_Array* tiledb_array;
	tiledb_array_init(
			tiledb_ctx, 
			&tiledb_array,
			arrayname,
			TILEDB_ARRAY_READ,
			NULL, 
			attributes,           
			1);

	uint64_t *buffer_coords = new uint64_t [2*readsize];
	void* buffers[] = { buffer_coords };
	size_t buffer_sizes[] = { 2*readsize*sizeof(uint64_t) };

	int rand_range = 10000;
	int64_t offset;
	float rt = 0.0;
	int64_t x0,y0;
	for (int i = 0; i < nqueries; i++) {
		offset = rand() % (2*rand_range) - rand_range;
		x0 = ((int64_t)(dim0_lo + offset) < 0) ? dim0_lo : dim0_lo + offset;
		y0 = ((int64_t)(dim1_lo + offset) < 0) ? dim1_lo : dim1_lo + offset;
		int64_t subarray[] = { x0, (int64_t)(dim0_hi + offset), y0, (int64_t)(dim1_hi + offset) };
		GETTIME(start);
		tiledb_array_reset_subarray(tiledb_array, subarray);
		/* Read from array. */
		if (tiledb_array_read(tiledb_array, buffers, buffer_sizes) != TILEDB_OK) {
			cerr << "error reading array\n";
			exit(EXIT_FAILURE);
		}
		GETTIME(end);
		rt += DIFF_TIME_SECS(start,end);
	}
	/* Finalize the array. */
	tiledb_array_finalize(tiledb_array);

	GETTIME(g_end);

	/* Finalize context. */
	tiledb_ctx_finalize(tiledb_ctx);

	MPI_Finalize();

	if (mpi_rank==0) {
		printf("total: %.3f\n", DIFF_TIME_SECS(g_start, g_end));
		printf("avg: %.3f\n", (DIFF_TIME_SECS(start, end)/nqueries));
	}

	if (toFileFlag==1) {
		int fd = open("temp_tiledb.bin", O_CREAT | O_WRONLY, S_IRWXU);
		write(fd, buffer_coords, 2*readsize*sizeof(uint64_t));
		close(fd);	
	}

	return 0;
}
