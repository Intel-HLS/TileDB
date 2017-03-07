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
#include <time.h>
#include <sys/time.h>
#include <map>
#include <sstream>
#include <vector>
#include <tiledb_tests.h>

using namespace std;

int enable_affinity = 0;
int coreid = -1;
int verbose = 0;
uint64_t dim_values[RANK*2];
int length = 0;
int srand_key = 0;
char *tiledb_arrayname = NULL;

void affinitize(int coreid);
int parse_opts(int argc, char **argv);

int main(
	int argc,
	char **argv) {

	parse_opts(argc, argv);

	if (enable_affinity)
  	affinitize(coreid);

	if (argc < 8) {
		std::cout << "Usage: " << argv[0] << " arrayname coreid dim0-lo dim0-hi dim1-lo dim1-hi length [srand_key]\n";
		exit(EXIT_FAILURE);
	}

  /* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
  tiledb_ctx_init(&tiledb_ctx, NULL);

  /* Subset over attribute "a1" and the coordinates. */
  const char* attributes[] = { "a1", TILEDB_COORDS };

  struct timeval start, end;
  clock_t t1, t2;

  /* Initialize the array in WRITE mode. */
  TileDB_Array* tiledb_array;
  GETTIME(start);
  t1 = clock();
  tiledb_array_init(
      tiledb_ctx, 
      &tiledb_array,
      tiledb_arrayname,
      TILEDB_ARRAY_WRITE_UNSORTED,
      NULL,            // No range - entire domain
      attributes,            // No projection - all attributes
      2);              // Meaningless when "attributes" is NULL
  t2 = clock();
  GETTIME(end);
	float inittime = DIFF_TIME_SECS(start,end);

	if (verbose) {
  	cout << "init wall time: " << inittime << " secs\n";
  	cout << "update init CPU time: " << (float) (t2-t1)/CLOCKS_PER_SEC
			<< " secs\n";
	}

  int64_t dim0_lo = dim_values[0];
  int64_t dim0_hi = dim_values[1];
  int64_t dim1_lo = dim_values[2];
  int64_t dim1_hi = dim_values[3];

	if (verbose) {
		std::cout << "Running with srand_key: " << srand_key << "\n";
	}

  /* Prepare cell buffers for attributes "a1" and "a2". */
  int buffer_a1[length];
  int64_t buffer_coords[2*length];
  const void* buffers[] = { buffer_a1, buffer_coords};
  size_t buffer_sizes[3] = { length*sizeof(int), 2*length*sizeof(int64_t) };

	srand(srand_key);

	std::map<std::string, int> myMap;
	std::map<std::string, int>::iterator it;
	myMap.clear();

  /* Populate attribute buffers with some arbitrary values. */
  // Random updates
  int64_t d0, d1, x;
  int64_t coords_index = 0L;
	int count = 0;
  for (int i = 0; i < length; ++i) {
		std::ostringstream mystream;
		do {
			d0 = dim0_lo + rand() % (dim0_hi - dim0_lo + 1);
			d1 = dim1_lo + rand() % (dim1_hi - dim1_lo + 1);
			x = rand() * (-1);
			std::ostringstream mystream;
			mystream << d0 << "," << d1;
			it = myMap.find(mystream.str());
		} while (it != myMap.end());
		mystream << d0 << "," << d1;
		myMap[mystream.str()] = x;
	  buffer_coords[coords_index++] = d0;
	  buffer_coords[coords_index++] = d1;
	  buffer_a1[i] = x;
		if (verbose) {
	  	std::cout << "(" << d0 << "," << d1 << "," << x << ")\n";
		}
		count++;
  }

	if (verbose) {
		std::cout << "count: " << count << "\n";
	}
  /* Write to array. */
  GETTIME(start);
  t1 = clock();
  tiledb_array_write(tiledb_array, buffers, buffer_sizes); 
  t2 = clock();
  GETTIME(end);
	float updatetime = DIFF_TIME_SECS(start,end);

  /* Finalize the array. */
  GETTIME(start);
  t1 = clock();
  tiledb_array_finalize(tiledb_array);
	system("sync");
  t2 = clock();
  GETTIME(end);
	float finaltime = DIFF_TIME_SECS(start,end);

	if (verbose) {
  	printf("Total time taken: %.3f\n", (inittime+updatetime+finaltime));
	}

  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;
}

void affinitize(int coreid) {
#ifdef __linux__
	cpu_set_t mask;
	int status;

	CPU_ZERO(&mask);
	CPU_SET(coreid, &mask);
	status = sched_setaffinity(0, sizeof(mask), &mask);
	if (status != 0)
	{
		std::cerr << "sched setaffinity error\n";
		exit(EXIT_FAILURE);
	}
#endif
}

/**
 * Parse command line parameters
 */
int parse_opts(
	int argc,
	char **argv) {

  int c, help = 0;
  opterr = 0;

	while ((c = getopt (argc, argv, "a:l:t:u:r:hv")) != -1) {
		switch (c)
		{
			case 'a':
				{
					tiledb_arrayname = new char [FILENAMESIZE];
					strcpy(tiledb_arrayname, optarg);
					break;
				}
			case 'l':
				{
					length = atoi(optarg);
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
					dim_values[0] = vect[0];
					dim_values[1] = vect[1];
					dim_values[2] = vect[2];
					dim_values[3] = vect[3];
					break;
				}
			case 'v':
				{
					verbose = 1;
					break;
				}
			case 'u':
				{
					coreid = atoi(optarg);
					break;
				}
			case 'r':
				{
					srand_key = atoi(optarg);
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
	error = (dim_values[0] == 0 && dim_values[1] == 0
						&& dim_values[2] == 0 && dim_values[3] == 0) ? 1 : error;

	if (error || help) {
		cout << "\n Usage: " << argv[0] << ":\n\n\t-a arrayname\t\tTileDB Array name/directory\n"
				<< "\n\t-t dim0-lo,dim0-hi,dim1-lo,dim1-hi\tLower and Upper values for each"
				<< "\n\t\t\t\t\t\tdimension where the updates will occur\n"
				<< "\n\t-l\t\t\tNumber of updates"
				<< "\n\t-r\t\t\tSeed of the random number generator"
				<< "\n\t-v\t\t\tVerbose to print all info and warning messages and update values"
				<< "\n\t[-u coreid]\t\tOptional core id to affinitize this process\n";
	
		exit(EXIT_FAILURE);
	}
	if (length == 0) {
		cout << "ERROR: No values will be updated with fragment length = 0\n";
		exit(EXIT_FAILURE);
	}
	return EXIT_SUCCESS;
}
