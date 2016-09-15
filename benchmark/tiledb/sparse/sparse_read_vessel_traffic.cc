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
 * Reads the AIS vessel traffic data. Check the schema of the table
 * defined in sparse_create_vessel_traffic.cc. In this script, we read only the
 * X,Y coordinates, i.e. scaled lat-longs. To read more data from TileDB,
 * add the attributes in the "attributes" buffer and also modify buffer_sizes
 * data structure accordingly. Further details on how to populate these buffers
 * can be found in core TileDB documentation
 */

#include "c_api.h"
#include <iostream>
#include <fstream>
#include <time.h>
#include <sys/time.h>
#include <cstdlib>
#include <string.h>
#include <tiledb_tests.h>

using namespace std;

int main(int argc, char **argv) {

  /* Initialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
	TileDB_Config config = {};
	config.read_method_ = TILEDB_IO_MMAP;
  tiledb_ctx_init(&tiledb_ctx, &config);

	if (argc < 10) {
		cout << "Usage: " << argv[0] << " arrayname dim0_lo dim0_hi dim1_lo "
				 << "dim1_hi n toFile nqueries seed\n";
		exit(EXIT_FAILURE);
	}
	char arrayname[10240];
	strcpy(arrayname, argv[1]);
	const uint64_t dim0_lo = atoll(argv[2]);
	const uint64_t dim0_hi = atoll(argv[3]);
	const uint64_t dim1_lo = atoll(argv[4]);
	const uint64_t dim1_hi = atoll(argv[5]);
	const int readsize = atoi(argv[6]);
	const int printFlag = atoi(argv[7]);
	const int nqueries = atoi(argv[8]);
	const int key = atoi(argv[9]);
	
	srand(key);

  /* Subset over attribute "a1" and the coordinates. */
  const char* attributes[] = { TILEDB_COORDS };

  struct timeval start, end;

  GETTIME(start);
  /* Initialize the array in READ mode. */
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx, 
      &tiledb_array,
      arrayname,
      TILEDB_ARRAY_READ,
      NULL, // range, 
      attributes,           
      1);
	GETTIME(end);
	float initt = DIFF_TIME_SECS(start, end);
  uint64_t *buffer_coords = new uint64_t [2*readsize];
  void* buffers[] = { buffer_coords };
  size_t buffer_sizes[] = { 2*readsize*sizeof(uint64_t) };

	clock_t t1,t2;
	float diff_clock = 0.0;
	int rand_range = 10000;
	int64_t offset;
	float rt = 0.0;
	uint64_t x0,y0;
	for (int i = 0; i < nqueries; i++) {
		offset = rand() % (2*rand_range) - rand_range;
		x0 = ((int64_t)(dim0_lo + offset) < 0) ? dim0_lo : dim0_lo + offset;
		y0 = ((int64_t)(dim1_lo + offset) < 0) ? dim1_lo : dim1_lo + offset;
		uint64_t subarray[] = { x0, dim0_hi + offset, y0, dim1_hi + offset };
		GETTIME(start);
		t1=clock();
		tiledb_array_reset_subarray(tiledb_array, subarray);
		/* Read from array. */
		if (tiledb_array_read(tiledb_array, buffers, buffer_sizes) != TILEDB_OK) {
			cerr << "error reading array\n";
			exit(EXIT_FAILURE);
		}
		t2=clock();
		GETTIME(end);
		rt += DIFF_TIME_SECS(start,end);
		diff_clock += (float)(t2-t1)/CLOCKS_PER_SEC;
	}

  printf("Average: %.3f\n", rt/nqueries);
  printf("Init: %.3f\n", initt);
  printf("Total: %.3f\n", initt+rt);
	printf("cpu time: %.3f\n", diff_clock);

  /* Print the read values. */
	if (printFlag==1) {	// to stdout
		for (int i=0;i<readsize;++i) {
			cout << buffer_coords[2*i] << "," << buffer_coords[2*i+1] << "\n";
		}
	} else if (printFlag==2) {	// to a text file
		cout << "printflag=" << printFlag << "\n";
		char filename[10240];
		strcpy(filename, argv[7]);
		ofstream of(filename);
		for (int i=0;i<readsize;++i) {
			of << buffer_coords[2*i] << "," << buffer_coords[2*i+1] << "\n";
		}
		of.close();
	} else if (printFlag==3) {	// to a binary file
		cout << "not yet\n";
	}

  /* Finalize the array. */
  tiledb_array_finalize(tiledb_array);

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}
