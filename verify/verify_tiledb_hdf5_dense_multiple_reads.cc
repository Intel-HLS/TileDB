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

#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdio>
#include <cstdlib>
#include <sys/types.h>
#include <H5Cpp.h>
#include <string.h>
#include <c_api.h>
#include <hdf5.h>
#include <fcntl.h>
#include <algorithm>
#include <tiledb_tests.h>
#include <vector>

using namespace std;
using namespace H5;

//////////////////////
// Global Variables //
//////////////////////
char *tiledb_arrayname = NULL;
char *hdf5_arrayname = NULL;
uint64_t read_sizes[RANK] = { 1000, 1000 };
int verbose = 0;
int coreid = 0;
int enable_affinity = 0;
int srand_key = 0;
int nqueries = 0;

//////////////////////
// Global Functions //
//////////////////////
int parse_opts(int argc, char **argv);
int get_array_info(TileDB_CTX* tiledb_ctx, uint64_t &dim0, uint64_t &dim1,
										uint64_t &chunkdim0, uint64_t &chunkdim1);
int check_reads(TileDB_CTX* tiledb_ctx, uint64_t dim0, uint64_t dim1);
void affinitize(int coreid);

/**
 * Print contents of an integer buffer
 */
void printall(int *, size_t);
/**
 * Print contents of two integer buffers
 * side by side (as gvimdiff)
 */
void print2all(int *, int *, size_t);

/**
 * Main point of entry
 */
int main(
	int argc,
	char **argv) {

	parse_opts(argc, argv);

	if (enable_affinity)
		affinitize(coreid);

	// Initialize the TileDB context
	TileDB_CTX *tiledb_ctx;
	TileDB_Config *config = new TileDB_Config;
	config->read_method_ = TILEDB_IO_MMAP;
	tiledb_ctx_init(&tiledb_ctx, config);

	uint64_t dim0, dim1, chunkdim0, chunkdim1;
	get_array_info(tiledb_ctx, dim0, dim1, chunkdim0, chunkdim1);
	check_reads(tiledb_ctx, dim0, dim1);

	// Finalize context
	tiledb_ctx_finalize(tiledb_ctx);

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

/**
 * Scan the TileDB array schema to extract
 * domain sizes and chunk sizes
 */
int get_array_info(
	TileDB_CTX* tiledb_ctx,
	uint64_t &dim0,
	uint64_t &dim1,
	uint64_t &chunkdim0,
	uint64_t &chunkdim1) {
	
	TileDB_ArraySchema array_schema;
  tiledb_array_load_schema(
      tiledb_ctx,
      tiledb_arrayname,
      &array_schema);
	uint64_t *dim_ranges = (uint64_t *) array_schema.domain_;
	dim0 = dim_ranges[1] - dim_ranges[0] + 1;
	dim1 = dim_ranges[3] - dim_ranges[2] + 1;
	uint64_t *tile_extents = (uint64_t *) array_schema.tile_extents_;
	chunkdim0 = tile_extents[0];
	chunkdim1 = tile_extents[1];
	// Free array schema
  tiledb_array_free_schema(&array_schema);
	return TILEDB_OK;
}

/**
 * Given the number of subarray queries
 * and read ranges, verify the results
 * with corresponding hyperslab query
 * in HDF5
 */
int check_reads(
	TileDB_CTX *tiledb_ctx,
	uint64_t dim0,
	uint64_t dim1) {

	int readsize_dim0 = read_sizes[0];
	int readsize_dim1 = read_sizes[1];

	srand(srand_key);

	// Initialize array
	TileDB_Array *tiledb_array;
	const char * attributes[] = { "a1" };

	int ret = tiledb_array_init(tiledb_ctx, &tiledb_array, tiledb_arrayname,	
			TILEDB_ARRAY_READ, NULL, attributes, 1);
	if (ret != TILEDB_OK) {
		cerr << "error initializing TileDB array\n";
		exit(EXIT_FAILURE);
	}	
	size_t buffersize = readsize_dim0 * readsize_dim1;
	int *tiledb_buffer = new int [buffersize];
	size_t buffersizes[] = {buffersize*sizeof(int)};

	hid_t acc_tpl1 = H5Pcreate (H5P_FILE_ACCESS);
	assert(acc_tpl1 != -1);
	H5Pset_cache(acc_tpl1, 0, 1000*400, 1000*1000*400, 0.75);
	FileAccPropList plist(acc_tpl1);
	// Open the file
	hid_t fid1=H5Fopen(hdf5_arrayname,H5F_ACC_RDWR,acc_tpl1);
	assert(fid1 != -1);
	ret=H5Pclose(acc_tpl1);
	assert(ret != -1);
	hid_t dapl_id = H5Pcreate(H5P_DATASET_ACCESS);
	/* set large chunk cache size */
	H5Pset_chunk_cache(dapl_id, 1000*400, 1000*1000*400, 0.75);
	// Open the dataset
	hid_t dataset1 = H5Dopen2(fid1, DATASETNAME, dapl_id);
	assert(dataset1 != -1);
	//DataSet* dataset = new DataSet(file->openDataSet(DATASETNAME));
	DataSet dataset(dataset1);
	DataSpace dataSpace = dataset.getSpace();

	int *hdf5_buffer = new int [buffersize];
	hsize_t memoffset[1] = {0};
	hsize_t memcount[1] = {buffersize};
	DataSpace memSpace(1, memcount);
	hsize_t offset[RANK], count[RANK];
	count[0] = readsize_dim0;
	count[1] = readsize_dim1;

	for (int i = 0; i < nqueries; i++) {
		uint64_t offset0 = rand() % (dim0 - readsize_dim0);
		uint64_t offset1 = rand() % (dim1 - readsize_dim1);

		void *buffers[] = {tiledb_buffer};

		uint64_t subarray[] = {offset0, offset0+readsize_dim0-1, offset1, offset1+readsize_dim1-1};
		if (verbose) {
			cout << "Query " << i << " running with ranges: " << subarray[0] << " " <<
				subarray[1] << " " << subarray[2] << " " << subarray[3] << "\n";
		}

		// Reset subarray
		tiledb_array_reset_subarray(tiledb_array, subarray);
		// Read from array
		if (tiledb_array_read(tiledb_array, buffers, buffersizes) != TILEDB_OK) {
			cout << "ERROR reading tiledb array\n";
			exit(EXIT_FAILURE);
		}

		sort(tiledb_buffer, tiledb_buffer+buffersize);

		offset[0] = offset0;
		offset[1] = offset1;

		dataSpace.selectHyperslab(H5S_SELECT_SET, count, offset);
		memSpace.selectHyperslab(H5S_SELECT_SET, memcount, memoffset);
		dataset.read(hdf5_buffer, PredType::NATIVE_INT, memSpace, dataSpace);

		sort(hdf5_buffer, hdf5_buffer+buffersize);

		for (size_t j = 0; j < buffersize; ++j) {
			if (tiledb_buffer[j] != hdf5_buffer[j]) {
				cout << "mismatch : " << tiledb_buffer[j] << "!=" << hdf5_buffer[j] << "\n";
				cout << "Query " << i << " running with ranges: " << subarray[0] << " " <<
					subarray[1] << " " << subarray[2] << " " << subarray[3] << "\n";
				cout << "TileDB coordinates (X,Y): " << (tiledb_buffer[j]/dim1) <<
					"," << (tiledb_buffer[j]%dim1) << "\n";
				cout << "HDF5 coordinates (X,Y): " << (hdf5_buffer[j]/dim1) <<
					"," << (hdf5_buffer[j]%dim1) << "\n";	
				if (verbose) {
					cout << "TileDB Array after sort\t\tHDF5 array after sort\n";
					print2all(tiledb_buffer, hdf5_buffer, buffersize);
				}	
				exit(EXIT_FAILURE);
			}
		}
	}
	/* Finalize the array. */
	tiledb_array_finalize(tiledb_array);
	
	dataSpace.close();
	dataset.close();

	return EXIT_SUCCESS;	
}	// check_reads

void printall(int *array, size_t size) {
	for (size_t i = 0; i < size; ++i)
		cout << array[i] << "\n";
}

void print2all(int *array1, int *array2, size_t size) {
	for (size_t i = 0; i < size; ++i)
		cout << array1[i] << "\t\t\t\t\t\t\t\t" << array2[i] << "\n";
}

/**
 * Parse the command line parameters
 */
int parse_opts(
	int argc,
	char **argv) {

	int c, help = 0;
	opterr = 0;

	while ((c = getopt (argc, argv, "a:b:k:n:r:u:hv")) != -1) {
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
					hdf5_arrayname = new char [FILENAMESIZE];
					strcpy(hdf5_arrayname, optarg);
					break;
				}
			case 'k':
				{
					srand_key = atoi(optarg);
					break;
				}
			case 'n':
				{
					nqueries = atoi(optarg);
					break;
				}
			case 'r':
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
					read_sizes[0] = vect[0];
					read_sizes[1] = vect[1];
					break;
				}
			case 'u':
				{
					enable_affinity=1;
					coreid = atoi(optarg);
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
				{
					cout << "ERROR: unknown option. Check with '-h' flag\n";
					exit(EXIT_FAILURE);
				}
		}
	}	// end of while

	int error = 0;
	error = (!tiledb_arrayname) ? 1 : error;
	error = (!hdf5_arrayname) ? 1 : error;
	error = (read_sizes[0] == 0 && read_sizes[1] == 0) ? 1 : error;
	error = (nqueries < 1) ? 1 : error;

	if (verbose) {
		cout << "\n TileDB(R) Single Read Verification Script - version " <<
				TILEDB_VERSION << "\n\n";
	}

	if (error || help) {
		cout << "\n Usage: " << argv[0]
			<< ":\n\n\t-a arrayname\t\tTileDB Array name/directory\n"
			<< "\n\n\t-b arrayname\t\tHDF5 Array name\n"
			<< "\n\n\t-d dim0-lo,dim0-hi,dim1-lo,dim1-hi\tRange of the subarray query"
			<< "\n\n\t-v\t\t\tVerbose"
			<< "\n\n\t[-u coreid]\t\tOptional core id to affinitize this process\n";

		exit(EXIT_FAILURE);
	}

	return EXIT_SUCCESS;	
}
