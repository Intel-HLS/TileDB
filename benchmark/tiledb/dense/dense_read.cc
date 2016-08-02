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
#include <time.h>
#include <sys/time.h>
#include <fcntl.h>
#include <cassert>
#include <cstdlib>
#include <tiledb_tests.h>
#include <vector>
#include <sstream>

using namespace std;

char *tiledb_arrayname;
uint64_t read_range[RANK*2];
int coreid = 0;
int enable_affinity = 0;
int toFileFlag = 0;

void affinitize(int coreid);
int parse_opts(int argc, char **argv);

int main(
	int argc,
	char **argv) {

	//int mpi_rank, mpi_size;
	//MPI_Init(&argc,&argv);
  //MPI_Comm comm = MPI_COMM_WORLD;
  //MPI_Info info = MPI_INFO_NULL;
  //MPI_Comm_size(comm, &mpi_size);
  //MPI_Comm_rank(comm, &mpi_rank);

	parse_opts(argc, argv);

	affinitize(coreid);

	/* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
	TileDB_Config config;
	config.read_method_ = TILEDB_IO_MMAP;

	// If you want to use read method = TILEDB_IO_MPI, enable the
	// commented MPI_Init above and MPI_Finalize below

	//config.mpi_comm_ = &comm;

  tiledb_ctx_init(&tiledb_ctx, &config);

  /* Subset over attribute "a1". */
  const char* attributes[] = { "a1" };

  struct timeval start, end;

  GETTIME(start);
  /* Initialize the array in READ mode. */
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx,
      &tiledb_array,
      tiledb_arrayname,
      TILEDB_ARRAY_READ,
      read_range,
      attributes,
      1);
  GETTIME(end);

	float inittime = DIFF_TIME_SECS(start,end);

  /* Prepare cell buffers for attributes "a1" and "a2". */
  size_t d0 = read_range[1] - read_range[0] + 1;
  size_t d1 = read_range[3] - read_range[2] + 1;
  size_t size = d0*d1;
  int *buffer_a1 = new int [size];
  void* buffers[] = { buffer_a1 };
  size_t buffer_sizes[] = { size*sizeof(int) };

  GETTIME(start);
  /* Read from array. */
  tiledb_array_read(tiledb_array, buffers, buffer_sizes);
  GETTIME(end);

	float readtime = DIFF_TIME_SECS(start,end);

  GETTIME(start);
  /* Finalize the array. */
  tiledb_array_finalize(tiledb_array);
  GETTIME(end);

	float finaltime = DIFF_TIME_SECS(start,end);
  printf("%.3f\n", (inittime+readtime+finaltime));

  if (toFileFlag == 1) {
	  int fd = open("temp_tiledb.bin", O_CREAT | O_WRONLY, S_IRUSR);
		write(fd, buffer_a1, size*sizeof(int));
		close(fd);
  } else if (toFileFlag==2) {
		ofstream of("temp_tiledb.csv");
		for (size_t i = 0;i < size; i++) {
			of << buffer_a1[i] << "\n";
		}
		of.close();
	}

	//MPI_Finalize();

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
		cerr << "sched setaffinity error\n";
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

	while ((c = getopt (argc, argv, "a:t:u:f:h")) != -1) {
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
					vector<uint64_t> vect;
					stringstream ss(optarg);
					int i;
					while (ss >> i)
					{
						vect.push_back(i);

						if (ss.peek() == ',')
							ss.ignore();
					}
					read_range[0] = vect[0];
					read_range[1] = vect[1];
					read_range[2] = vect[2];
					read_range[3] = vect[3];
					break;
				}
			case 'u':
				{
					enable_affinity = 1;
					coreid = atoi(optarg);
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
			default:
				abort ();
		}
	}	// end of while

	int error = 0;
	error = (!tiledb_arrayname) ? 1 : error;
	error = (read_range[0] == 0 && read_range[1] == 0
						&& read_range[2] == 0 && read_range[3] == 0) ? 1 : error;

	if (error || help) {
		cout << "\n Usage: " << argv[0]
			  << ":\n\n\t-a arrayname\t\tTileDB Array name/directory\n"
				<< "\n\t-t dim0-lo,dim0-hi,dim1-lo,dim1-hi\tSubarray range\n"
			  << "\n\t-f 1/2\t\t\tOptional to file flag"
			  << "\n\t\t\t\t=1 means subarray will be dumped as binary file"
				<< "\n\t\t\t\t=2 means subarray will be dumped as CSV file\n"
				<< "\n\t[-u coreid]\t\tOptional core id to affinitize this process\n";
	
		exit(EXIT_FAILURE);
	}
	return EXIT_SUCCESS;
}
