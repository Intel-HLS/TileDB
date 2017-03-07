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
#include <string>
#include <cstring>
#include <vector>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <tiledb_tests.h>
#include <map>
#include <sstream>

using namespace std;

char *tiledb_arrayname = NULL;
char *filename = NULL;
int enable_affinity = 0;
int coreid = 0;
int ncells = 0;
int verify = 0;
int verbose = 0;

void affinitize(int coreid);
int parse_opts(int argc, char **argv);

int main(int argc, char **argv) {

	parse_opts(argc, argv);

	if (enable_affinity)
		affinitize(coreid);

	char command[10240], sorted_filename[10240];
	sprintf(sorted_filename, "%s_sorted", filename);
	sprintf(command, "sort -g -k1 -k2 -k3 %s > %s", filename, sorted_filename);
	system(command);

	struct timeval start, end;
	GETTIME(start);

// Initialize context with the default configuration parameters
	TileDB_CTX* tiledb_ctx;
	TileDB_Config *config = new TileDB_Config;
	config->read_method_ = TILEDB_IO_READ;
	tiledb_ctx_init(&tiledb_ctx, config);

	// Load array schema when the array is not initialized
  TileDB_ArraySchema array_schema;
  tiledb_array_load_schema(
      tiledb_ctx,
      tiledb_arrayname,
      &array_schema);
	uint64_t* domain = (uint64_t*) array_schema.domain_;
	uint64_t dim1 = domain[3] - domain[2] + 1;
	// Free array schema
  tiledb_array_free_schema(&array_schema);

	uint64_t r, c;
	uint64_t buffer_dim[ncells][2];

	FILE *fp = fopen(sorted_filename,"r");
	if (fp==NULL) {
		cerr << "file open error\n";
		exit(EXIT_FAILURE);
	}
	int index = 0;
	int id;
	while(fscanf(fp,"%d %ld %ld",&id, &r,&c)!=EOF) {
		buffer_dim[index][0] = r;
		buffer_dim[index][1] = c;
		index++;
		if (index > ncells) {
			cout << "ERROR: Number of cells do not match with given file\n";
			exit(EXIT_FAILURE);
		}
	}
	fclose(fp);

	if (index != ncells) {
		cerr << "ERROR: number of cell doesn't match with lines in file\n";
		exit(EXIT_FAILURE);
	}

	int *buffer = new int [ncells];
	size_t buffersizes[] = {1*sizeof(int)};
	TileDB_Array *tiledb_array;
	const char * attributes[] = { "a1" };
	// Initialize array
	int ret = tiledb_array_init(tiledb_ctx, &tiledb_array, tiledb_arrayname,	
				TILEDB_ARRAY_READ, NULL, attributes, 1);

	if (ret != TILEDB_OK) {
		cerr << "error initializing TileDB array\n";
		exit(EXIT_FAILURE);
	}

	for (int i = 0; i < ncells; ++i) {
		void *buffers[] = {buffer+i};
		uint64_t subarray[] = {buffer_dim[i][0], buffer_dim[i][0],
			buffer_dim[i][1], buffer_dim[i][1]};

		// Reset subarray
		tiledb_array_reset_subarray(tiledb_array, subarray);
		// Read from array
		if (tiledb_array_read(tiledb_array, buffers, buffersizes) != TILEDB_OK) {
			cout << "ERROR reading tiledb array\n";
			pthread_exit(NULL);
		}

		if (verify==1) {
			if (buffer[i] != (int)(buffer_dim[i][0]*dim1+buffer_dim[i][1])) {
				cerr << "no match\n";
				exit(EXIT_FAILURE);
			}
			if (verbose) {
				printf("%d==[%ld,%ld]\n", buffer[i], buffer_dim[i][0], buffer_dim[i][1]);
			}
		}
		if (verbose) {
			cout << buffer_dim[i][0] << " " <<
				buffer_dim[i][1] << " " << buffer[i] << "\n";
		}
	}

  /* Finalize the array. */
  tiledb_array_finalize(tiledb_array);
	// Finalize context
	tiledb_ctx_finalize(tiledb_ctx);
	GETTIME(end);

	if (verbose) {
		printf("total time taken: %.3f\n", DIFF_TIME_SECS(start, end));
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

	while ((c = getopt (argc, argv, "a:f:u:n:hbv")) != -1) {
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
					filename = new char [FILENAMESIZE];
					strcpy(filename, optarg);
					break;
				}
			case 'u':
				{
					coreid = atoi(optarg);
					break;
				}
			case 'n':
				{
					ncells = atoi(optarg);
					break;
				}
			case 'b':
				{
					verify = 1;
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
	error = (!filename) ? 1 : error;
	error = (ncells==0) ? 1 : error;

	if (error || help) {
		cout << "\n Usage: " << argv[0]
				<< ":\n\n\t-a arrayname\tTileDB Array name/directory\n"
				<< "\n\t-f filename\tFile containing coordinates of the random reads"
				<< "\n\t-n Integer\tNumber of cells to be read\n"
				<< "\n\t[-u coreid]\tOptional core id to affinitize this process"
				<< "\n\t[-b]\t\tOptional flag to verify contents of random reads"
				<< "\n\t[-v]\t\tVerbose to print values and info messages\n";
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

