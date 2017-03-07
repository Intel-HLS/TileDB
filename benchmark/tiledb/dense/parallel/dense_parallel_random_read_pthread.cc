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
#include <map>
#include <sstream>
#include <tiledb_tests.h>

using namespace std;

struct mystruct {
	int id;
	char arrayname[FILENAMESIZE];
	TileDB_CTX *tiledb_ctx;
	int *buffer_dim0;
	int *buffer_dim1;
	size_t *buffersizes;
	int ncells;
	int sum;
};

char *tiledb_arrayname = NULL;
int verbose = 0;
int ncells = 0;
int nthreads = 0;
int srand_key = 0;

void *pthread_read(void *args);
int parse_opts(int argc, char **argv);

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
	const int dim0 = domain[1] - domain[0] + 1;
	const int dim1 = domain[3] - domain[2] + 1;

	srand(srand_key);

	std::map<std::string, int> myMap;
	std::map<std::string, int>::iterator it;
	myMap.clear();

	int r, c, v;
	struct mystruct thread_data[nthreads];
	int	buffer_dim0[nthreads][ncells];
	int	buffer_dim1[nthreads][ncells];

	for (int i = 0; i < nthreads; ++i) {
		thread_data[i].buffer_dim0 = new int [ncells];
		thread_data[i].buffer_dim1 = new int [ncells];
		for (int j = 0; j < ncells; ++j) {
			std::ostringstream mystream;
			do {
				r = rand() % dim0;
				c = rand() % dim1;
				v = rand() * (-1);
				std::ostringstream mystream;
				mystream << r << "," << c;
				it = myMap.find(mystream.str());
			} while (it!=myMap.end());
			if (verbose) {
				mystream << r << "," << c;
			}
			myMap[mystream.str()] = v;
			buffer_dim0[i][j] = r;
			buffer_dim1[i][j] = c;
			thread_data[i].buffer_dim0[j] = buffer_dim0[i][j];
			thread_data[i].buffer_dim1[j] = buffer_dim1[i][j];
		}
		strcpy(thread_data[i].arrayname, tiledb_arrayname);
	}

	pthread_t threads[nthreads];
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

	struct timeval start, end;

	GETTIME(start);
	for (int i = 0; i < nthreads; ++i) {
		thread_data[i].id = i;
		thread_data[i].tiledb_ctx = tiledb_ctx;
		thread_data[i].ncells = ncells;
		thread_data[i].sum = 0;
		pthread_create(&threads[i], &attr, pthread_read, &thread_data[i]);
	}
	for (int i = 0; i < nthreads; ++i) {
		void *status = 0;
		pthread_join(threads[i], &status);
	}
	GETTIME(end);

	// Finalize context
	tiledb_ctx_finalize(tiledb_ctx);

	if (verbose) {
		printf("read time: %.3f\n", DIFF_TIME_SECS(start, end));
	}
	return EXIT_SUCCESS;
}

void *pthread_read(void *args) {

	struct mystruct *readArgs = (struct mystruct *) args;

	struct timeval start, end;
	int *buffer = new int [readArgs->ncells];
	float time = 0.0;

	TileDB_Array *tiledb_array;
	const char * attributes[] = { "a1" };
	GETTIME(start);

	// Initialize array
	tiledb_array_init(readArgs->tiledb_ctx, &tiledb_array, readArgs->arrayname,	
			TILEDB_ARRAY_READ, NULL, attributes, 1);

	for (int i = 0; i < readArgs->ncells; ++i) {
		void *buffers[] = {buffer+i};
		size_t buffersizes[] = {1*sizeof(int)};
		int64_t subarray[] = {readArgs->buffer_dim0[i], readArgs->buffer_dim0[i],
			readArgs->buffer_dim1[i], readArgs->buffer_dim1[i]};

		tiledb_array_reset_subarray(tiledb_array, subarray);
		// Read from array
		if (tiledb_array_read(tiledb_array, buffers, buffersizes) != TILEDB_OK) {
			cout << "ERROR writing tiledb array\n";
			pthread_exit(NULL);
		}
		if (verbose) {
			char command[10240];
			sprintf(command, "thread: %d [%ld,%ld]=%d, size=%ld\n", readArgs->id, subarray[0], subarray[2], buffer[i],buffersizes[0]);
			cout << command;
		}
	}

	// Finalize the TileDB array
	tiledb_array_finalize(tiledb_array);

	GETTIME(end);
	time += DIFF_TIME_SECS(start,end);

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

	while ((c = getopt (argc, argv, "a:t:n:vh")) != -1) {
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
	error = (nthreads < 1) ? 1 : error;
	error = (ncells < 1) ? 1 : error;

	if (error || help) {
		cout << "\n Usage: " << argv[0] << ":\n\n\t-a arrayname\t\tTileDB Array name/directory\n"
				<< "\n\t-t Integer\t\tNumber of threads"
				<< "\n\t-n Integer\t\tNumber of cells read per thread\n";
		exit(EXIT_FAILURE);
	}
	return EXIT_SUCCESS;
}
