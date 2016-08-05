/**
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
#include <vector>
#include <tiledb_tests.h>

using namespace std;
using namespace H5;

//////////////////////
// Global Variables //
//////////////////////
char *tiledb_arrayname = NULL;
char *hdf5_arrayname = NULL;
char *datadir = NULL;
uint64_t dim_values[RANK] = { 0, 0 };
uint64_t chunk_sizes[RANK] = { 0, 0 };
int verbose = 0;
int coreid = 0;
int enable_affinity = 0;
int compress = 0;
int capacity = 1000000;	// Default capacity of TileDB is set to 1M
int nchunks = 0;
int nqueries = 0;
int nfrags = 0;

// Default fragment size = 1000. Note this value is
// ignored when nfrags = 0
int fragsize = 1000;
int srand_key = 0;

// Default read range is 1000,1000. Note that this is not used
// if nqueries = 0
uint64_t read_range[RANK] = { 1000, 1000 };

//////////////////////
// Global Functions //
//////////////////////
int parse_opts(int argc, char **argv);
int read_binary_chunks(int **chunks);
int load_tiledb(int **chunks);
int load_hdf5(int **chunks);
int create_multi_fragments();
int update_hdf5_array();
int check_reads();
void printall(int *, size_t);
void affinitize(int coreid);

/**
 * Main point of entry
 */
int main(int argc, char **argv) {

	parse_opts(argc, argv);

	if (enable_affinity)
		affinitize(coreid);

	nchunks = (dim_values[0]/chunk_sizes[0])*(dim_values[1]/chunk_sizes[1]);
	int **chunks = (int **) malloc(sizeof(int*)*nchunks);
	size_t chunk_byte_size = chunk_sizes[0] * chunk_sizes[1] * sizeof(int);
	for (int i=0;i<nchunks;++i)
		chunks[i] = (int *) malloc(chunk_byte_size);

	read_binary_chunks(chunks);
	load_tiledb(chunks);
	load_hdf5(chunks);
	create_multi_fragments();
	update_hdf5_array();
	check_reads();

	return EXIT_SUCCESS;
}

/**
 * The expectation here is that the array is already
 * written chunk by chunk to binary files and the
 * input directory pointing to the folder is provided
 * in the command line. This method reads chunk by
 * chunk and stores them in a temporary buffer
 */
int read_binary_chunks(
	int **chunks) {

	size_t chunk_byte_size = chunk_sizes[0] * chunk_sizes[1] * sizeof(int);
	char filename[FILENAMESIZE];
	int elements = 0;
	int bytes = 0;
	for (int i=0;i<nchunks;++i) {
		sprintf(filename, "%s/chunk%d.bin", datadir, i);
		if (verbose)
			printf("Reading file: %s...\n", filename);
		
		int fd = open(filename, O_RDONLY);
		if (fd != -1) {
			bytes = read(fd, (void*)chunks[i], chunk_byte_size);
		} else {
			printf("Unable to open file: %s\n", filename);
			exit(EXIT_FAILURE);
		}
		close(fd);
		elements += bytes / sizeof(int);
	}	

	if (verbose)
		printf("%d elements read completed\n", elements);
	return 0;
}	// read_binary_chunks

/**
 * Load the array to TileDB
 * chunk by chunk
 */
int load_tiledb(
	int **chunks) {

	uint64_t dim0 = dim_values[0];
	uint64_t dim1 = dim_values[1];
	uint64_t chunkdim0 = chunk_sizes[0];
	uint64_t chunkdim1 = chunk_sizes[1];

	char command[FILENAMESIZE];
	sprintf(command,"rm -rf %s", tiledb_arrayname);
	system(command);
	// Initialize context with the default configuration parameters
	TileDB_CTX* tiledb_ctx;
	tiledb_ctx_init(&tiledb_ctx, NULL);

	const int attribute_num = 1;
	const char* attributes[] = { "a1" };
	const char* dimensions[] = { "X", "Y" };
	uint64_t domain[] = { 0, dim0-1, 0, dim1-1 };
	uint64_t tile_extents[] = { chunkdim0, chunkdim1 };
	const int types[] = { TILEDB_INT32, TILEDB_INT64 };
	
	int compression[2];

	if (compress == 0) {
		compression[0] = TILEDB_NO_COMPRESSION;
		compression[1] = TILEDB_NO_COMPRESSION;
	} else {
		compression[0] = TILEDB_GZIP;
		compression[1] = TILEDB_GZIP;
	}
	const int dense = 1;
	const int cell_val_num[] = { 1 };

  TileDB_ArraySchema array_schema;

	tiledb_array_set_schema(&array_schema, tiledb_arrayname, attributes,
			attribute_num, capacity, TILEDB_ROW_MAJOR, cell_val_num,
			compression, dense, dimensions, RANK, domain, 4*sizeof(int64_t),
			tile_extents, 2*sizeof(int64_t), TILEDB_ROW_MAJOR, types);

  /* Create the array. */
  tiledb_array_create(tiledb_ctx, &array_schema);	

	// Array name
	if (verbose)
		cout << "Blockcount: " << nchunks << "\n";

	// Read chunks to local buffers
	//int *chunks[blockcount];
	int buffer_size = chunkdim0 * chunkdim1 * sizeof(int);

	struct timeval start, end;

	GETTIME(start);
	// Initialize array
	TileDB_Array* tiledb_array;
	tiledb_array_init(
			tiledb_ctx,
			&tiledb_array,
			tiledb_arrayname,
			TILEDB_ARRAY_WRITE,
			NULL,
			NULL,
			0);

	for (int i = 0; i < nchunks; ++i) {
		const void * buffers[] = {chunks[i]};
		size_t buffer_sizes[] = {(size_t)buffer_size};

		// Write to array
		tiledb_array_write(tiledb_array, buffers, buffer_sizes);
	}
	// Finalize array
	tiledb_array_finalize(tiledb_array);
	system("sync");
	GETTIME(end);

	// Finalize context
	tiledb_ctx_finalize(tiledb_ctx);

	if (verbose)
		printf("TileDB load time: %.3f\n", DIFF_TIME_SECS(start,end));

	return EXIT_SUCCESS;	
}	// load_tiledb

/**
 * Load the array to HDF5
 * chunk by chunk
 */
int load_hdf5(
	int **chunks) {

	uint64_t dim0 = dim_values[0];
	uint64_t dim1 = dim_values[1];
	uint64_t chunkdim0 = chunk_sizes[0];
	uint64_t chunkdim1 = chunk_sizes[1];

	if (verbose)
		cout << "HDF5 Filename: " << hdf5_arrayname << "\n";

	hid_t       file_id, dset_id;    /* file and dataset identifiers */
	hid_t       filespace, memspace; /* file and memory dataspace identifiers */
	hsize_t     dimsf[2];            /* dataset dimensions */
	hsize_t	count[2];	          /* hyperslab selection parameters */
	hsize_t	offset[2];
	hsize_t stride[2];	
	hid_t	plist_id;
	herr_t status;

	try
	{
		plist_id = H5Pcreate(H5P_FILE_ACCESS);
		/*
		 * Create a new file collectively and release property list identifier.
		 */
		file_id = H5Fcreate(hdf5_arrayname, H5F_ACC_TRUNC, H5P_DEFAULT, plist_id);

		/*
		 * Create the dataspace for the dataset.
		 */
		dimsf[0] = dim0;
		dimsf[1] = dim1;
		filespace = H5Screate_simple(2, dimsf, NULL); 

		/*
		 * Create the dataset creation property lits
		 */
		hid_t dcpl_id = H5Pcreate(H5P_DATASET_CREATE);
		assert(dcpl_id!=-1);
		hsize_t chunkdims[2] = {chunkdim0, chunkdim1};
		H5Pset_chunk(dcpl_id, 2, chunkdims);
		H5Pset_alloc_time(dcpl_id, H5D_ALLOC_TIME_INCR);
		if (compress) {
			H5Pset_deflate(dcpl_id, 6); // -- not supported in PHDF5 - 1.10.0
		}

		/*
		 * Create the dataset with default properties and close filespace.
		 */
		dset_id = H5Dcreate(file_id, DATASETNAME, H5T_NATIVE_INT, filespace,
				H5P_DEFAULT, dcpl_id, H5P_DEFAULT);
		assert(dset_id != FAIL);
		H5Pclose(dcpl_id);
		H5Sclose(filespace);

		uint64_t dim0_lo, dim0_hi, dim1_lo, dim1_hi;
		float time = 0.0;
		struct timeval start, end;

		/* create a memory dataspace independently */
		hsize_t mcount[] = {1,(chunkdim0 * chunkdim1)};
		memspace = H5Screate_simple (2, mcount, NULL);
		assert (memspace != FAIL);

		for (int i = 0; i < nchunks; ++i) {
			int y = i%(dim1/chunkdim1);
			int x = (i - y)/(dim1/chunkdim1);
			dim0_lo = x * chunkdim0;
			dim0_hi = dim0_lo + chunkdim0 - 1;
			dim1_lo = y * chunkdim1;
			dim1_hi = dim1_lo + chunkdim1 - 1;
			offset[0] = dim0_lo;
			offset[1] = dim1_lo;
			count[0] = dim0_hi-dim0_lo+1;
			count[1] = dim1_hi-dim1_lo+1;
			stride[0] = 1;
			stride[1] =1;

			/*
			 * Select hyperslab in the file.
			 */
			filespace = H5Dget_space(dset_id);
			H5Sselect_hyperslab(filespace, H5S_SELECT_SET, offset, stride, count, NULL);

			/*
			 * Create property list for collective dataset write.
			 */
			plist_id = H5Pcreate(H5P_DATASET_XFER);

			GETTIME(start);
			status = H5Dwrite(dset_id, H5T_NATIVE_INT, memspace, filespace,
					plist_id, chunks[i]);
			assert(status != FAIL);
			GETTIME(end);
			time += DIFF_TIME_SECS(start, end);
			H5Sclose(filespace);
			H5Pclose(plist_id);
		}	// end of for block

		if (verbose)
			cout << "\nBlocks written: " << nchunks << "\n";
		GETTIME(start);
		/*
		 * Close/release resources.
		 */
		H5Dclose(dset_id);
		H5Fclose(file_id);	
		system("sync");
		GETTIME(end);
		float finalize_time = DIFF_TIME_SECS(start, end);
		if (verbose) {
			cout << "finalize time: " << finalize_time << " secs\n";
			cout << "total write time: " << (finalize_time + time) << " secs\n";
		}
	}  // end of try block

	// catch failure caused by the H5File operations
	catch(FileIException error)
	{
		error.printError();
		return -1;
	}

	// catch failure caused by the DataSet operations
	catch(DataSetIException error)
	{
		error.printError();
		return -1;
	}

	// catch failure caused by the DataSpace operations
	catch(DataSpaceIException error)
	{
		error.printError();
		return -1;
	}
	return 0;
}	// load_hdf5

/**
 * Generate random coordinates and random values
 * to update the array.
 * IMPORTANT: Remember to reset the random number
 *            generator before calling this method
 *            so that same sequence of random numbers
 *            are generated in two subsequent calls
 */ 
void fill_random_values(
	uint64_t dim0,
	uint64_t dim1, 
	int length,
	uint64_t *buffer_coords,
	int *buffer_a1,
	int verify,
	int fragid,
	int verbose) {

	if (verbose)
		cout << "Filling random values:::\n";
  map<string, int> myMap;
  map<string, int>::iterator it;
  myMap.clear();

	int64_t d0, d1, x;
	int64_t coords_index = 0L;

	char filename[1024];
	sprintf(filename,"mf_update_entires_%d.csv", fragid);
	ofstream of(filename);

	for (int i = 0; i < length; ++i) {
		ostringstream mystream;
		do {
			d0 = rand() % dim0;
			d1 = rand() % dim1;
			x = 0 - (d0*dim1+d1);
			ostringstream mystream;
			mystream << d0 << "," << d1;
			it = myMap.find(mystream.str());
		} while (it != myMap.end());
		mystream << d0 << "," << d1;
		myMap[mystream.str()] = x;
		buffer_coords[coords_index++] = d0;
		buffer_coords[coords_index++] = d1;
		buffer_a1[i] = x;
		if (verbose)
			cout << "(" << d0 << "," << d1 << "," << x << ")\n";
		if (verify) {
			of << "(" << d0 << "," << d1 << "," << x << ")\n";
		}
	}
	of.close();
}

/**
 * Given number of fragments and size of fragments
 * this updates them to TileDB. Note that this is
 * where we reset the random number generator
 */
int create_multi_fragments() {

	srand(srand_key);
	
	/* Intialize context with the default configuration parameters. */
  TileDB_CTX* tiledb_ctx;
	TileDB_Config config = {};
	config.read_method_ = TILEDB_IO_MMAP;
  tiledb_ctx_init(&tiledb_ctx, &config);

  /* Subset over attribute "a1" and the coordinates. */
  const char* attributes[] = { "a1", TILEDB_COORDS };

  struct timeval start, end;

  /* Prepare cell buffers for attributes "a1" and "a2". */
  int buffer_a1[fragsize];
  uint64_t buffer_coords[2*fragsize];
  const void* buffers[] = {buffer_a1, buffer_coords};
  size_t buffer_sizes[3] = { fragsize*sizeof(int),
														 2*fragsize*sizeof(uint64_t) };

  float diff_secs = 0.0;

	int count = 0;
	for (int f = 0; f < nfrags; ++f) {
		/* Initialize the array in WRITE mode. */
		TileDB_Array* tiledb_array;
		GETTIME(start);
		tiledb_array_init(
				tiledb_ctx,
				&tiledb_array,
				tiledb_arrayname,
				TILEDB_ARRAY_WRITE_UNSORTED,
				NULL,            // No range - entire domain
				attributes,            // No projection - all attributes
				2);              // Meaningless when "attributes" is NULL
		GETTIME(end);
		float inittime = DIFF_TIME_SECS(start,end);

		/* Populate attribute buffers with some arbitrary values. */
		fill_random_values(dim_values[0], dim_values[1], fragsize,
						buffer_coords, buffer_a1, 1, f, verbose);
		/* Write to array. */
		GETTIME(start);
		tiledb_array_write(tiledb_array, buffers, buffer_sizes);
		GETTIME(end);
		float wt = DIFF_TIME_SECS(start,end);

		/* Finalize the array. */
		GETTIME(start);
		system("sync");
		tiledb_array_finalize(tiledb_array);
		GETTIME(end);
		float ft = DIFF_TIME_SECS(start,end);

		diff_secs += (inittime + wt + ft);
		count++;
	}

	if (verbose) {
		printf("%.3f\n", diff_secs);
		cout << count << " fragments written\n";
	}
  /* Finalize context. */
  tiledb_ctx_finalize(tiledb_ctx);

  return 0;	
}	// create_multi_fragments

/**
 * Update HDF5 array with same values as
 * are created in TileDB. Note that this is
 * where we reset the random number generator
 */
int update_hdf5_array() {

	srand(srand_key);

	/*
	 * HDF5 APIs definitions
	 */ 	
	hid_t       file_id, dset_id;    /* file and dataset identifiers */
	hid_t       filespace, memspace; /* file and memory dataspace identifiers */
	hid_t	plist_id;                 /* property list identifier */
	herr_t	status;

	int *buffer_a1 = (int *) malloc(sizeof(int)*fragsize);

	struct timeval g_start, g_end;

	GETTIME(g_start);

	plist_id = H5Pcreate(H5P_FILE_ACCESS);
	file_id = H5Fopen(hdf5_arrayname, H5F_ACC_RDWR, plist_id);
	assert(file_id!=-1);
	//H5Pclose(plist_id);
	hid_t dapl_id = H5Pcreate(H5P_DATASET_ACCESS);
	/* set large chunk cache size */
	H5Pset_chunk_cache(dapl_id, 1000*400, 8000000000, 0.75);
	dset_id = H5Dopen(file_id, DATASETNAME, dapl_id);  
	assert(dset_id!=-1);
	//H5Pclose(dapl_id);
	filespace = H5Dget_space(dset_id);

	hsize_t dims[]={(hsize_t)fragsize};
	memspace = H5Screate_simple(1,dims, NULL);
	uint64_t *coord_buffer = (uint64_t *) malloc(sizeof(uint64_t)*2*fragsize);
	float time = 0.0;
	
	for (int f = 0; f < nfrags; f++) {

		/* Populate attribute buffers with some arbitrary values. */
		fill_random_values(dim_values[0], dim_values[1], fragsize,
				coord_buffer, buffer_a1, 1, f, verbose);	
		/*
		 * Select hyperslab in the file.
		 */
		H5Sselect_elements(filespace, H5S_SELECT_SET, fragsize,
				(const hsize_t*)coord_buffer);
		/*
		 * Create property list for collective dataset write.
		 */
		//plist_id = H5Pcreate(H5P_DATASET_XFER);
		//H5Pset_dxpl_mpio(plist_id, H5FD_MPIO_COLLECTIVE);

		struct timeval start, end;
		GETTIME(start);
		status = H5Dwrite(dset_id, H5T_NATIVE_INT, memspace, filespace,
				H5P_DEFAULT, buffer_a1);
		assert(status != FAIL);
		GETTIME(end);
		time += DIFF_TIME_SECS(start, end);
	}
	printf("pure write time: %.3f seconds\n", time);
	/*
	 * Close/release resources.
	 */
	H5Dclose(dset_id);
	H5Sclose(filespace);
	H5Sclose(memspace);
	H5Pclose(plist_id);
	H5Fclose(file_id);
	//system("sync");

	GETTIME(g_end);
	float g_time = DIFF_TIME_SECS(g_start, g_end);

	//printf("join after barrier time: %.3f secs\n", g_time);
	GETTIME(g_start);
	system("sync");
	GETTIME(g_end);
	float s_time = DIFF_TIME_SECS(g_start, g_end);

	//printf("sync time: %.3f secs\n", s_time);
	printf("%.3f\n", g_time+s_time);
	return 0;	
}	// update_hdf5_array

/**
 * Given the number of subarray queries
 * and read ranges, verify the results
 * with corresponding hyperslab query
 * in HDF5
 */
int check_reads() {

	uint64_t dim0 = dim_values[0];
	uint64_t dim1 = dim_values[1];
	uint64_t readsize_dim0 = read_range[0];
	uint64_t readsize_dim1 = read_range[1];

	// Initialize context with the default configuration parameters
	TileDB_CTX* tiledb_ctx;
	TileDB_Config *config = new TileDB_Config;
	config->read_method_ = TILEDB_IO_MMAP;
	tiledb_ctx_init(&tiledb_ctx, config);

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
	H5Pset_cache(acc_tpl1, 0, 1000*400, 4000000000, 0.75);
	FileAccPropList plist(acc_tpl1);
	// Open the file
	hid_t fid1=H5Fopen(hdf5_arrayname,H5F_ACC_RDWR,acc_tpl1);
	assert(fid1 != -1);
	ret=H5Pclose(acc_tpl1);
	assert(ret != -1);
	hid_t dapl_id = H5Pcreate(H5P_DATASET_ACCESS);
	H5Pset_chunk_cache(dapl_id, 1000*400, 4000000000, 0.75);
	// Open the dataset
	hid_t dataset1 = H5Dopen2(fid1, DATASETNAME, dapl_id);
	assert(dataset1 != -1);
	//DataSet* dataset = new DataSet(file->openDataSet(DATASETNAME));
	DataSet dataset(dataset1);
	//cout << "storage size: " << dataset->getStorageSize() << "\n";
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

		uint64_t subarray[] =
				{offset0, offset0+readsize_dim0-1, offset1, offset1+readsize_dim1-1};
		if (verbose) {
			cout << "Query " << i << " running with ranges: " << subarray[0] << ","
					 << subarray[1] << "," << subarray[2] << "," << subarray[3] << "\n";
		}
		// Reset subarray
		tiledb_array_reset_subarray(tiledb_array, subarray);
		// Read from array
		if (tiledb_array_read(tiledb_array, buffers, buffersizes) != TILEDB_OK) {
			cout << "ERROR reading tiledb array\n";
			exit(EXIT_FAILURE);
		}
	
		sort(tiledb_buffer, tiledb_buffer+buffersize);
		if (verbose) {
			cout << "TileDB Array after sort: \n";
			printall(tiledb_buffer, buffersize);
		}
		offset[0] = offset0;
		offset[1] = offset1;

		dataSpace.selectHyperslab(H5S_SELECT_SET, count, offset);
		memSpace.selectHyperslab(H5S_SELECT_SET, memcount, memoffset);
		dataset.read(hdf5_buffer, PredType::NATIVE_INT, memSpace, dataSpace);

		sort(hdf5_buffer, hdf5_buffer+buffersize);
		if (verbose) {
			cout << "HDF5 Array after sort: \n";
			printall(hdf5_buffer, buffersize);
		}
		for (size_t j = 0; j < buffersize; ++j) {
			if (tiledb_buffer[j] != hdf5_buffer[j]) {
				cout << "mismatch : " << tiledb_buffer[j]
						 << "!=" << hdf5_buffer[j] << "\n";
				exit(EXIT_FAILURE);
			}
		}
	}
	/* Finalize the array. */
	tiledb_array_finalize(tiledb_array);
	// Finalize context
	tiledb_ctx_finalize(tiledb_ctx);
	dataSpace.close();
	dataset.close();

	return EXIT_SUCCESS;	
}	// check_reads

void printall(int *array, size_t size) {
	for (size_t i = 0; i < size; ++i)
		cout << array[i] << "\n";
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
 * Parse the command line parameters
 */
int parse_opts(
	int argc,
	char **argv) {

	int c, help = 0;
	opterr = 0;

	while ((c = getopt (argc, argv, "a:b:c:d:e:f:g:k:s:u:q:r:hvz")) != -1) {
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
			case 'c':
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
					chunk_sizes[0] = vect[0];
					chunk_sizes[1] = vect[1];
					break;
				}
			case 'd':
				{
					datadir = new char [FILENAMESIZE];
					strcpy(datadir, optarg);
					break;
				}
			case 'e':
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
			case 'f':
				{
					nfrags = atoi(optarg);
					break;
				}
			case 'g':
				{
					fragsize = atoi(optarg);
					break;
				}
			case 'k':
				{
					srand_key = atoi(optarg);
					break;
				}
			case 'q':
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
					read_range[0] = vect[0];
					read_range[1] = vect[1];
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
			case 's':
				{
					capacity = atoi(optarg);
					break;
				}
			case 'v':
				{
					verbose = 1;
					break;
				}
			case 'z':
				{
					compress = 1;
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
	error = (!datadir) ? 1 : error;
	error = (dim_values[0] == 0 && dim_values[1] == 0) ? 1 : error;
	error = (chunk_sizes[0] == 0 && chunk_sizes[1] == 0) ? 1 : error;
	error = (capacity < 1) ? 1 : error;

	cout << "\n TileDB(R) Verification Script - version " << TILEDB_VERSION << "\n\n";

	if (error || help) {
		cout << "\n Usage: " << argv[0]
			<< ":\n\n\t-a arrayname\t\tTileDB Array name/directory\n"
			<< "\n\n\t-b arrayname\t\tHDF5 Array name\n"
			<< "\n\n\t-c chunk size0, chunk size1\nChunk sizes in dimensions 0 and 1 of the array"
			<< "\n\n\t-d path\t\t\tDirectory containing the binary chunk files"
			<< "\n\n\t-e dim0,dim1\tDimension values of the array (used by both TileDB and HDF5"
			<< "\n\n\t-f Integer\t\tNumber of fragments"
			<< "\n\n\t-g Integer\t\tSize of fragments"
			<< "\n\n\t-k Integer\t\tRandom seed"
			<< "\n\n\t-q Integer\t\tNumber of queries"
			<< "\n\n\t-r Integer,Integer\tRead ranges"
			<< "\n\n\t-z\t\t\tEnable compression while creating both TileDB and HDF5 arrays"
			<< "\n\n\t-s Integer\t\tSpecify capacity for TileDB"
			<< "\n\n\t[-u coreid]\t\tOptional core id to affinitize this process\n";

		exit(EXIT_FAILURE);
	}

	cout << " Input Parameters :-\n\n";
	cout << "\tTiledb Array: " << tiledb_arrayname << "\n";
	cout << "\tHDF5 Array: " << hdf5_arrayname << "\n";
	cout << "\tDatadir: " << datadir << "\n";
	cout << "\tCapacity: " << capacity << "\n";
	cout << "\tCompression: " << compress << "\n";
	cout << "\tHelp is enabled\n";
	cout << "\tCore id for affinity: " << coreid << "\n";
	cout << "\tDimensions: " << dim_values[0] << ","
		<< dim_values[1] << "\n";
	cout << "\tChunk sizes: " << chunk_sizes[0] << ","
		<< chunk_sizes[1] << "\n";
	cout << "\tNumber of queries: " << nqueries << "\n";
	cout << "\tRead ranges: " << read_range[0] << ","
		<< read_range[1] << "\n";
	cout << "\tVerbose: " << verbose << "\n";
	cout << "\tRandom seed: " << srand_key << "\n";
	cout << "\tNumber of fragments: " << nfrags << "\n";
	cout << "\tSize of each fragment: " << fragsize << "\n";

	return EXIT_SUCCESS;	
}
