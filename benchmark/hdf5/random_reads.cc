#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <cstdlib>
#include <sstream>
#include <vector>
#include <time.h>
#include <sys/time.h>

#include "H5Cpp.h"

#ifdef __linux__
#include <sched.h>
#endif

// To compile: h5c++ loaddata.cc

using namespace std;
#ifndef H5_NO_NAMESPACE
    using namespace H5;
#endif

const H5std_string	FILE_NAME("/data1/hdf5/tiledb_dset.h5");
const H5std_string	DATASET_NAME("tiledb_dset");

const int	FSPACE_RANK = 2;          // Dataset rank in file
const int FSPACE_DIM1 = 5;         // dim1 size
const int FSPACE_DIM2 = 10;         // dim2 size
const int MSPACE_RANK = 2;    // Rank of the first dataset in memory
const int MSPACE_DIM1 = 2;    // We will read dataset back from the file
const int MSPACE_DIM2 = 2;    // to the dataset in memory with these

const long SEGMENTSIZE = 1000000;

int read(const std::string& filename, int length) {

	const H5std_string FILE_NAME(filename);

	//hid_t       plist_id;
	//plist_id = H5Pcreate(H5P_FILE_ACCESS);
	//H5Pset_cache(plist_id, sss * 2, sss * 2, sizeof(int) * sss * 2, 0);
	// Open the file
	H5File* file = new H5File(FILE_NAME, H5F_ACC_RDONLY);
	// Open the dataset
	DataSet* dataset = new DataSet(file->openDataSet(DATASET_NAME));
	//std::cout << "storage size: " << dataset->getStorageSize() << "\n";
	DataSpace dataSpace = dataset->getSpace();
	//int rank = dataSpace.getSimpleExtentNdims();
	//hsize_t dims_out[2];
	//int ndims = dataSpace.getSimpleExtentDims( dims_out, NULL);
	//std::cout << "rank " << rank << ", dimensions " <<
	//					(unsigned long)(dims_out[0]) << " x " <<
	//					(unsigned long)(dims_out[1]) << std::endl;

	// Get creation properties list.
	DSetCreatPropList cparms = dataset->getCreatePlist();

	// Get file access properties list
	FileAccPropList plist = file->getAccessPlist();
	//H5Pset_cache(plist.getId(), sss * 2, sss * 2, sizeof(int) * sss * 2, 0);

	const int myDIM1 = 1000*600;
	const int myDIM2 = 1000*5;

	struct timeval start, end;
	hsize_t stride[FSPACE_RANK], block[FSPACE_RANK];
	hsize_t offset[FSPACE_RANK], count[FSPACE_RANK];
	float secs = 0.0;
	float clocks = 0.0;

	int *buffer = (int *) malloc(sizeof(int) * 1 * 1);
	//H5Pset_hypercache(1048576*1024);
	hsize_t memoffset[2] = { 0, 0 };
	count[0] = 1;
	count[1] = 1;
	DataSpace memSpace(FSPACE_RANK, count);
		clock_t t1, t2;

	srand(0);
	for (int i = 0; i < length; ++i) {
		offset[0] = rand() % 200000; // myDIM1;
		offset[1] = rand() % 2500; // myDIM2;
		count[0] = 1; // readDim1;
		count[1] = 1; //readDim2;
		stride[0] = 1;
		stride[1] = 1;
		block[0] = 1;
		block[1] = 1;

		system("/home/user/workspace/clean_caches.sh");
		//std::cout << offset[0] << "," << offset[1] << "\n";
		gettimeofday(&start, NULL);
		t1 = clock();
		dataSpace.selectHyperslab(H5S_SELECT_SET, count, offset);
		memSpace.selectHyperslab(H5S_SELECT_SET, count, memoffset);
		dataset->read(buffer, PredType::NATIVE_INT, memSpace, dataSpace);
		t2 = clock();
		gettimeofday(&end, NULL);
		secs += (float)((end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec))/1000000;
		clocks += (float) (t2-t1)/CLOCKS_PER_SEC;
	}
	dataSpace.close();
	dataset->close();

	std::cout << "read wall clock time: " << secs << " secs\n";
	std::cout << "read CPU time: " << clocks << " secs\n";

	//gettimeofday(&start, NULL);
	//t1=clock();
	//buffer[0] = buffer[0] + 5;
	//t2=clock();
	//gettimeofday(&end, NULL);
	//std::cout << "buf access time: " << (float)((end.tv_sec - start.tv_sec) * 1000000 +
	//	(end.tv_usec - start.tv_usec))/1000000 << " secs+" << buffer[0] << "\n";
	//std::cout << "buf cpu time: " << (float) (t2-t1)/CLOCKS_PER_SEC << "\n";

	//int size = readDim1 * readDim2;
	//std::cout << "number of elements read: " << size << "\n";
	//std::cout << "first 5: " << buffer[0] << "," << buffer[1] << "," << buffer[2] << "," << buffer[3] << "," << buffer[4] << "\n";
	//std::cout << "last 5: " << buffer[size - 5] << "," << buffer[size - 4] << "," << buffer[size - 3] << "," << buffer[size - 2] <<
	//		"," << buffer[size - 1] << "\n";
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
	if (argc < 4) {
		std::cerr << "Usage: " << argv[0] << " <input-hdf5-filename> <coreid> <length>\n";
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

	const int length = atoi(argv[3]);
	//const int offset1 = atoi(argv[3]);
	//const int offset2 = atoi(argv[4]);
	//const int readDim1 = atoi(argv[5]);
	//const int readDim2 = atoi(argv[6]);

	//std::cout << "Running with readDim1: " << readDim1 << ", readDim2: " << readDim2 << 
	//  " offset1: " << offset1 << " offset2: " << offset2 << "\n";
	//const int myDIM1 = 16;
	//const int myDIM2 = 55377408;
	//const int myDIM1 = 4;
	//const int myDIM2 = 4;
	
	read(filename, length); // , readDim1, readDim2, offset1, offset2);
	return 0;
}
