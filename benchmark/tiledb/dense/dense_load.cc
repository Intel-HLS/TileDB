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
#include <tiledb_tests.h>
#include <sstream>

using namespace std;

#define RANK 2

char *tiledb_arrayname = NULL;
uint64_t dim_values[RANK] = { 0, 0 };
uint64_t tile_extents[RANK*2] = { 0, 0, 0, 0 };
int capacity = 1000000;
int enable_compression = 0;
int coreid = 0;
int enable_affinity = 0;

int parse_opts(int argc, char **argv);
void affinitize(int coreid);
int **generatedBuffer(const int myDIM1, const int myDIM2);
void writeToTileDB(long write_size, int * buffer_a1, int &blockcount,
		long &diff_usecs, float &diff_clock, TileDB_Array* tiledb_array);

int main(
	int argc,
	char **argv) {

	parse_opts(argc, argv);

	if (enable_affinity) {
		cout << "this process affinitized to " << coreid << "\n";
		affinitize(coreid);
	}

	/* Initialize context with the default configuration parameters. */
	TileDB_CTX* tiledb_ctx;
	if (tiledb_ctx_init(&tiledb_ctx, NULL) != TILEDB_OK) {
		cout << "ERROR initialising TileDB context\n";
		exit(EXIT_FAILURE);
	}

	/* Initialize the array in WRITE mode. */
	TileDB_Array* tiledb_array;
	struct timeval start, end;
	GETTIME(start);
	if (tiledb_array_init(
			tiledb_ctx,
			&tiledb_array,
			tiledb_arrayname,
			TILEDB_ARRAY_WRITE,
			NULL,            // No range - entire domain
			NULL,            // No projection - all attributes
			0)              // Meaningless when "attributes" is NULL
			!= TILEDB_OK) {
		cout << "Error initializing TileDB array\n";
		exit(EXIT_FAILURE);
	}
	GETTIME(end);
	std::cout << "init time: " << DIFF_TIME_SECS(start, end) << " secs\n";

	/* Populate buffers from CSV file */
	const int myDIM1 = dim_values[0];
	const int myDIM2 = dim_values[1];
	int **buffer = generatedBuffer(myDIM1, myDIM2);
	std::cout << "Buffer size in memory: " <<
		(double)((long)myDIM1*myDIM2*sizeof(int))/((long)1024*1024*1024) <<
		" GB\n";

	int blockcount = 0;

	int d0_extent = (tile_extents[1] - tile_extents[0] + 1);
	int d1_extent = (tile_extents[3] - tile_extents[2] + 1);

	std::cout << "array dimensions " << myDIM1 << "x" << myDIM2 << "\n";
	std::cout << "tile extents: " <<
			d0_extent << "x" << d1_extent << "\n";

	/* Prepare cell buffers for attributes "a1". */

	long segmentSize = (long)d0_extent * d1_extent;
	std::cout << "Intermediate buffer size in memory: " <<
		(double) ((long)segmentSize*sizeof(int))/((long)1024*1024*1024) << " GB\n";
	int * buffer_a1 = new int [segmentSize];
	//const void* buffers[RANK] = { buffer_a1 };
	//size_t buffer_sizes[RANK] = { segmentSize*sizeof(int) };
	uint64_t index = 0;
	float diff_clock = 0.0;
	long diff_usecs = 0L;

	int i, j;

	for (i = 0; i < myDIM1; i += d0_extent) {
		for (j = 0; j < myDIM2; j += d1_extent) {
			long tile_rows = ((i + d0_extent) < myDIM1) ? d0_extent : (myDIM1 - i);
			long tile_cols = ((j + d1_extent) < myDIM2) ? d1_extent : (myDIM2 - j);
			int k,l;
			for (k = 0; k < tile_rows; ++k) {
				for (l = 0; l < tile_cols; ++l) {
					index = uint64_t(k * tile_cols + l);
					buffer_a1[index] = buffer[uint64_t(i + k)][uint64_t(j + l)];
				}
			}
			long write_size = k*l* sizeof(int);
			writeToTileDB(write_size, buffer_a1, blockcount, diff_usecs, diff_clock,
				tiledb_array);
		}
	}

	std::cout << "write count: " << blockcount << "\n";
	std::cout << "write time: " << (float)diff_usecs/1000000 << " secs\n";
	std::cout << "write CPU time: " << diff_clock << " secs\n";

	GETTIME(start);
	/* Finalize the array. */
	tiledb_array_finalize(tiledb_array);
	system("sync");
	GETTIME(end);
	float finalize_time = DIFF_TIME_SECS(start,end);
	std::cout << "finalize wall time: " << finalize_time << " secs\n";
	std::cout << "total write wall clock time: " << (float)diff_usecs/1000000 + finalize_time << " secs\n";

	/* Finalize context. */
	tiledb_ctx_finalize(tiledb_ctx);

	return 0;
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

/**
 * Generate a two dimensional array where
 * each cell = row_id * COLS + col_id
 */
int **generatedBuffer(const int myDIM1, const int myDIM2) {
	int **buffer = new int * [myDIM1];
	for (int i = 0; i < myDIM1; ++i) {
		buffer[i] = new int [myDIM2];
		for (int j = 0; j < myDIM2; ++j) {
			buffer[i][j] = i * myDIM2 + j;
		}
		if (i != 0 && i%100000==0) std::cout << i << "\n";
	}
	return buffer;
}

/**
 * Write contents of a buffer to TileDB
 */
void writeToTileDB(long write_size, int * buffer_a1, int &blockcount, long &diff_usecs, float &diff_clock, TileDB_Array* tiledb_array) {

	const int twogb = 2*1000*1000*1000;
	clock_t t1, t2;
	struct timeval start, end;

	if (write_size <= twogb) {
		const void* buffers[RANK] = { buffer_a1 };
		size_t buffer_sizes[RANK] = { (size_t) write_size };
		GETTIME(start);
		t1 = clock();
		tiledb_array_write(tiledb_array, buffers, buffer_sizes);
		t2 = clock();
		GETTIME(end);
		diff_usecs += ((end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec));
		diff_clock += (float) (t2 - t1)/CLOCKS_PER_SEC;
		++blockcount;
	} else {
		int s = 0;
		do {
			/* Write to array. */
			const void* buffers[RANK] = { buffer_a1+s };
			size_t blocksize = (write_size > twogb) ? twogb : write_size;
			size_t buffer_sizes[RANK] = { blocksize };
			GETTIME(start);
			t1 = clock();
			tiledb_array_write(tiledb_array, buffers, buffer_sizes);
			t2 = clock();
			GETTIME(end);
			diff_usecs += ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));
			diff_clock += (t2 - t1);
			++blockcount;
			s += blocksize/sizeof(int);
			write_size -= blocksize;
			std::cout << blocksize << " written. Write_size = " << write_size << "\n";
		} while (write_size > 0);
	}
}	// writeToTileDB

/**
 * Parse command line parameters
 */
int parse_opts(
	int argc,
	char **argv) {

  int c, help = 0;
  opterr = 0;

	while ((c = getopt (argc, argv, "a:d:c:s:t:u:zh")) != -1) {
		switch (c)
		{
			case 'a':
				{
					tiledb_arrayname = new char [FILENAMESIZE];
					strcpy(tiledb_arrayname, optarg);
					break;
				}
			case 'd':
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
			case 't':
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
					tile_extents[0] = vect[0];
					tile_extents[1] = vect[1];
					tile_extents[2] = vect[2];
					tile_extents[3] = vect[3];
					break;
				}
			case 's':
				{
					capacity = atoi(optarg);
					break;
				}
			case 'z':
				{
					enable_compression = 1;
					break;
				}
			case 'u':
				{
					enable_affinity = 1;
					coreid = atoi(optarg);
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
	error = (dim_values[0] == 0 && dim_values[1] == 0) ? 1 : error;
	error = (tile_extents[0] == 0 && tile_extents[1] == 0
						&& tile_extents[2] == 0 && tile_extents[3] == 0) ? 1 : error;

	if (error || help) {
		cout << "\n Usage: " << argv[0] << ":\n\n\t-a arrayname\t\tTileDB Array name/directory\n"
				<< "\n\t-t dim0-lo,dim0-hi,dim1-lo,dim1-hi\tTile extents are the lower and upper"
				<< "\n\t\t\t\t\t\tranges of each tile in the array\n"
				<< "\n\t-d dim0,dim1\t\tDomain values"
				<< "\n\t[-u coreid]\t\tOptional core id to affinitize this process\n";
	
		exit(EXIT_FAILURE);
	}
	return EXIT_SUCCESS;
}
