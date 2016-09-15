#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <cstdlib>
#include <sstream>
#include <vector>
#include <time.h>
#include <sys/time.h>
#include <fcntl.h>
#include <unistd.h>
#include <tiledb_tests.h>
#include "hdf5.h"

//#include "H5Cpp.h"

// To compile: h5c++ loaddata.cc

using namespace std;
//#ifndef H5_NO_NAMESPACE
//    using namespace H5;
//#endif

#define FAIL -1

void toFile(const char *filename, int *buffer, const size_t size) {
	int fd = open(filename, O_WRONLY | O_CREAT, S_IRWXU);
	if (write(fd, (void *)buffer, size)==-1) {
		cout << "File write error\n";
		exit(EXIT_FAILURE);
	}
	close(fd);
}

int read(char *filename, const int dim0, const int dim1, const int chunkdim0,
		const int chunkdim1, const int nchunks, const int toFileFlag) {

	int ret;
	int total_chunks_in_array = (dim0/chunkdim0)*(dim1/chunkdim1);
	
	struct timeval starttime, endtime;

	// Open the file
	hid_t fid=H5Fopen(filename,H5F_ACC_RDONLY,H5P_DEFAULT);
	assert(fid != FAIL);

	hid_t dapl_id = H5Pcreate(H5P_DATASET_ACCESS);
	ret = H5Pset_chunk_cache(dapl_id, total_chunks_in_array, 2*dim0*dim1*sizeof(int), 0);
	assert(ret != FAIL);

	GETTIME(starttime);
	// Open the dataset
	hid_t dataset_id = H5Dopen2(fid, DATASETNAME, dapl_id);
	assert(dataset_id != FAIL);

	/* Release file-access template */
	ret=H5Pclose(dapl_id);
	assert(ret != FAIL);
	
	hsize_t start[RANK], count[RANK];
	int dim0_lo, dim0_hi, dim1_lo, dim1_hi;
	
	start[0] = 0;
	start[1] = 0;

	for (int i = 0; i < nchunks; ++i) {
		int y = i%(dim1/chunkdim1);
		int x = (i - y)/(dim1/chunkdim1);
		dim0_lo = x * chunkdim0;
		dim0_hi = dim0_lo + chunkdim0 - 1;
		dim1_lo = y * chunkdim1;
		dim1_hi = dim1_lo + chunkdim1 - 1;
		count[0] = dim0_hi+1;
		count[1] = dim1_hi+1;
	}

	/* create a file dataspace independently */
	hid_t file_dataspace = H5Dget_space (dataset_id);
	assert(file_dataspace != FAIL);
	ret=H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET, start, NULL,
			count, NULL);
	assert(ret != FAIL);

	/* create a memory dataspace independently */
	hid_t mem_dataspace = H5Screate_simple (RANK, count, NULL);
	assert (mem_dataspace != FAIL);

	int *buffer = (int *) malloc(sizeof(int)*count[0]*count[1]);

	/* read data independently */
	ret = H5Dread(dataset_id, H5T_NATIVE_INT, mem_dataspace, file_dataspace,
			H5P_DEFAULT, buffer);
	assert(ret != FAIL);

	/* close dataset */
	ret=H5Dclose(dataset_id);
	assert(ret != FAIL);

	/* release all IDs created */
	H5Sclose(file_dataspace);

	/* close the file collectively */
	H5Fclose(fid);	

	GETTIME(endtime);

	if (toFileFlag) {
		char filename[1024];
		sprintf(filename, "./tmp/chunk_read_results_chunk%d.bin", 0);
		printf("writing to file: %s\n", filename);
		toFile(filename, buffer, count[0]*count[1]*sizeof(int));
	}

	free(buffer);		
	printf("%.3f\n", DIFF_TIME_SECS(starttime, endtime));
	return 0;
}

int main (int argc, char **argv)
{
	if (argc < 6) {
		std::cerr << "Usage: " << argv[0] << " input-hdf5-filename dim0 " <<
			"dim1 chunkdim0 chunkdim1 nchunks toFile\n";
		exit(EXIT_FAILURE);
	}
	
	char filename[1024];
	strcpy(filename, argv[1]);
	const int dim0 = atoi(argv[2]);
	const int dim1 = atoi(argv[3]);
	const int chunkdim0 = atoi(argv[4]);
	const int chunkdim1 = atoi(argv[5]);
	const int nchunks = atoi(argv[6]);
	const int toFileFlag = atoi(argv[7]);

	read(filename, dim0, dim1, chunkdim0, chunkdim1, nchunks, toFileFlag);
	return 0;
}
