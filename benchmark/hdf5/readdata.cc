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

#include "H5Cpp.h"
#include <hdf5.h>

#ifdef __linux__
#include <sched.h>
#endif

// To compile: h5c++ loaddata.cc

using namespace std;
#ifndef H5_NO_NAMESPACE
    using namespace H5;
#endif

void toFile(const char *filename, int *buffer, const int readDim1, const int readDim2) {
	int fd = open(filename, O_WRONLY | O_CREAT, S_IRWXU);
	size_t size = readDim1*readDim2*sizeof(int);
	if (write(fd, (void *)buffer, size)==-1) {
		cout << "File write error\n";
		exit(EXIT_FAILURE);
	}
	close(fd);
}

int read(const std::string& filename, const int readDim1,
		const int readDim2, const hsize_t offset1, const hsize_t offset2, const int toFileFlag) {

	const H5std_string FILE_NAME(filename);

	//hid_t       plist_id;
	//plist_id = H5Pcreate(H5P_FILE_ACCESS);
	struct timeval start, end;

	GETTIME(start);
	// Get file access properties list
	/* setup file access template */
	hid_t acc_tpl1 = H5Pcreate (H5P_FILE_ACCESS);
	assert(acc_tpl1 != -1);
	H5Pset_cache(acc_tpl1, 0, 400000, 4000000000, 0.75);
	FileAccPropList plist(acc_tpl1);
	// Open the file
	hid_t fid1=H5Fopen(filename.c_str(),H5F_ACC_RDWR,acc_tpl1);
	assert(fid1 != -1);
	int ret=H5Pclose(acc_tpl1);
	assert(ret != -1);
	//H5File* file = new H5File(FILE_NAME, H5F_ACC_RDONLY, &plist);
	hid_t dapl_id = H5Pcreate(H5P_DATASET_ACCESS);
	/* set large chunk cache size */
	//size_t nslots, nbytes;
	//double w0;
	//H5Pget_chunk_cache(dapl_id, &nslots, &nbytes, &w0);
	//printf("Before: %ld %ld %f\n", nslots, nbytes, w0);
	H5Pset_chunk_cache(dapl_id, 1000*400, 4000000000, 0.75);
	// Open the dataset
	hid_t dataset1 = H5Dopen2(fid1, DATASETNAME, dapl_id);
	assert(dataset1 != -1);
	//DataSet* dataset = new DataSet(file->openDataSet(DATASETNAME));
	DataSet dataset(dataset1);
	//std::cout << "storage size: " << dataset->getStorageSize() << "\n";
	DataSpace dataSpace = dataset.getSpace();
	//hid_t file_dataspace = H5Dget_space (dataset1);	
	GETTIME(end);
	float inittime = DIFF_TIME_SECS(start,end);

	hsize_t offset[RANK], count[RANK];
	offset[0] = offset1;
	offset[1] = offset2;
	count[0] = readDim1;
	count[1] = readDim2;

	//int buffer[readDim1][readDim2];
	//int **buffer = (int **) malloc(sizeof(int *) * readDim1);
	//for (int i = 0; i < readDim1; ++i) {
	//  buffer[i] = (int *) malloc(sizeof(int) * readDim2);
	//	for (int j = 0; j < readDim2; ++j) {
	//		buffer[i][j] = 0;
	//	}
	//}
	size_t buffersize = readDim1 * readDim2;
	int *buffer = new int [buffersize];
	//H5Pset_hypercache(1048576*1024);
	hsize_t memoffset[1] = {0};
	hsize_t memcount[1] = {buffersize};
	DataSpace memSpace(1, memcount);

	GETTIME(start);
	dataSpace.selectHyperslab(H5S_SELECT_SET, count, offset);
	memSpace.selectHyperslab(H5S_SELECT_SET, memcount, memoffset);
	dataset.read(buffer, PredType::NATIVE_INT, memSpace, dataSpace);
	dataSpace.close();
	dataset.close();
	GETTIME(end);

	float readtime = DIFF_TIME_SECS(start,end);
	printf("%.3f\n", (inittime+readtime));

	if (toFileFlag==1) {
		toFile("temp_hdf5.bin", buffer, readDim1, readDim2);
	}
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
	}
#endif
}

int main (int argc, char **argv)
{
	if (argc < 8) {
		std::cerr << "Usage: " << argv[0] << " input-hdf5-filename coreid offset1 offset2 readDim1 readDim2 toFile\n";
		exit(EXIT_FAILURE);
	}
	std::string filename(argv[1]);
	std::ifstream fs(filename.c_str());
	if (!fs.is_open()) {
		std::cerr << filename << " No such file or directory\n";
		exit(EXIT_FAILURE);		 
	}
	fs.close();

	affinitize(atoi(argv[2]));
	const int offset1 = atoi(argv[3]);
	const int offset2 = atoi(argv[4]);
	const int readDim1 = atoi(argv[5]);
	const int readDim2 = atoi(argv[6]);
	const int toFileFlag = atoi(argv[7]);

	//std::cout << "Running with readDim1: " << readDim1 << ", readDim2: " << readDim2 << 
	//  " offset1: " << offset1 << " offset2: " << offset2 << "\n";
	
	read(filename, readDim1, readDim2, offset1, offset2, toFileFlag);
	return 0;
}
