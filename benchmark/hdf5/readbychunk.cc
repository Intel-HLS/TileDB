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

int read(const H5std_string& filename, const int readDim1,
				 const int readDim2, const hsize_t offset1, const hsize_t offset2,
				 const hsize_t chunkDim1, const hsize_t chunkDim2) {

	hid_t       plist_id;
	plist_id = H5Pcreate(H5P_FILE_ACCESS);
  // Open the file
	H5File* file = new H5File(filename, H5F_ACC_RDONLY, plist_id);
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
	// Check if dataset is chunked.
	int rank_chunk;
	hsize_t chunk_dims[2] = {chunkDim1, chunkDim2};
	
	struct timeval start, end;
	hsize_t stride[FSPACE_RANK], block[FSPACE_RANK];
	//hsize_t offset[FSPACE_RANK], count[FSPACE_RANK];
	//offset[0] = offset1;
	//offset[1] = offset2;
	//count[0] = chunk_dims[0]; // readDim1;
	//count[1] = chunk_dims[1]; // readDim2;
	stride[0] = 1;
	stride[1] = 1;
	block[0] = 1;
	block[1] = 1;

	int **buffer = (int **) malloc(readDim1 * sizeof(int *));
	for (int i = 0; i < readDim1; ++i) {
	  buffer[i] = (int *) malloc(readDim2 * sizeof(int));
	}
	//H5Pset_hypercache(1048576*1024);
  //hsize_t memoffset[2] = { 0, 0 };
  //DataSpace memSpace(FSPACE_RANK, count);
	int sss = readDim1 * readDim2;
	//dataSpace.selectHyperslab(H5S_SELECT_SET, count, offset, stride, block);
	//dataSpace.selectHyperslab(H5S_SELECT_SET, count, offset);
	//memSpace.selectHyperslab(H5S_SELECT_SET, count, memoffset);
	//H5Pset_cache(plist_id, sss * 2, sss * 2, sizeof(int) * sss * 2, 0);
	clock_t t1, t2;
	long diff_clocks = 0L;
	long diff_usecs = 0L;
	long diff_timeofday = 0L;
	int index = 0;

	int temp_buffer[chunkDim1][chunkDim2];
	//int ** temp_buffer = (int **) malloc(chunkDim1 * sizeof(int *));
	//for (int i = 0; i < chunkDim1; ++i) {
	//	temp_buffer[i] = (int *) malloc(chunkDim2 * sizeof(int));
	//}
	hsize_t offset[FSPACE_RANK], count[FSPACE_RANK];
	hsize_t memoffset[2] = { 0, 0 };

	for (int i = offset1; i < (offset1 + readDim1); i += chunkDim1) {
		for (int j = offset2; j < (readDim2 + offset2); j += chunkDim2) {
			offset[0] = i;
			offset[1] = j;
			count[0] = chunkDim1;
			count[1] = chunkDim2;
			DataSpace memSpace(FSPACE_RANK, count);
			dataSpace.selectHyperslab(H5S_SELECT_SET, count, offset);
			memSpace.selectHyperslab(H5S_SELECT_SET, count, memoffset);
			gettimeofday(&start, NULL);
			t1 = clock();
			dataset->read(temp_buffer, PredType::NATIVE_INT, memSpace, dataSpace);
			t2 = clock();
			gettimeofday(&end, NULL);
			diff_timeofday += ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));
			//std::cout << "read:" << i << "," << j << "\n";
			diff_clocks += (t2 - t1);
			for (int k = 0; k < chunkDim1 && k < readDim1; ++k) {
				for (int l = 0; l < chunkDim2 && l < readDim2; ++l) {
					//index = (i * chunk_dims[0]) + (k * chunk_dims[0]) + (j * chunk_dims[1]) + l;
					//std::cout << temp_buffer[k][l] << "\n";
					//std::cout << "k: " << k << "," << l << "=" << temp_buffer[k * chunk_dims[0] + l];
					//std::cout << x << "," << y << "=" << buffer[x][y] << "\n";
					buffer[i + k][j + l] = temp_buffer[k][l];
					//int c = getchar();
				}
				//std::cout << k << "\n";
			}
		}
	}
	
	dataSpace.close();
	dataset->close();

	std::cout << "read time: " << (float) diff_usecs/1000000 << " secs\n";
	std::cout << "read wall clock time: " << (float) diff_timeofday / 1000000 << " secs\n"; 
	std::cout << "read CPU time: " << (float) diff_clocks / CLOCKS_PER_SEC << " secs\n";

  //for (int i = 0; i < readDim1; ++i) {
	//	for (int j = 0; j < readDim2; ++j) {
	//	  std::cout << "buffer[" << i << "][" << j << "]=" << buffer[i][j] << "  ";
	//	}
	//}

  //for (int i = 0; i < (readDim1 * readDim2); ++i) {
	//  std::cout << buffer[i] << " ";
	//}

  int size = readDim1 * readDim2;
	std::cout << buffer[0][0] << "," << buffer[0][1] << "," << buffer[0][2] << "," << buffer[0][3] <<
		"," << buffer[0][4] << "\n";
	std::cout << buffer[readDim1-1][readDim2 - 5] << "," << buffer[readDim1-1][readDim2 - 4] << "," <<
		buffer[readDim1-1][readDim2 - 3] <<	"," << buffer[readDim1-1][readDim2 - 2] << "," <<
		buffer[readDim1-1][readDim2 - 1] << "\n";
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
	if (argc < 9) {
		std::cerr << "Usage: " << argv[0] << " <input-hdf5-filename> <coreid> " << 
			"<chunkDim1> <chunkDim2> " <<
			"<offset1> <offset2> " <<
			"<readDim1> <readDim2>\n";
		exit(EXIT_FAILURE);
	}
	std::string filename(argv[1]);
	const H5std_string	FILE_NAME(filename);
	std::ifstream fs(filename.c_str());
	if (!fs.is_open()) {
		std::cerr << filename << " No such file or directory\n";
		exit(EXIT_FAILURE);		 
	}
	fs.close();

	affinitize(atoi(argv[2]));
	const int chunkDim1 = atoi(argv[3]);
	const int chunkDim2 = atoi(argv[4]);
	const int offset1 = atoi(argv[5]);
	const int offset2 = atoi(argv[6]);
	const int readDim1 = atoi(argv[7]);
	const int readDim2 = atoi(argv[8]);

	std::cout << "Running with readDim1: " << readDim1 << ", readDim2: " << readDim2 << 
	  " offset1: " << offset1 << " offset2: " << offset2 << "\n";

	const int myDIM1 = 16;
	const int myDIM2 = 55377408;
	//const int myDIM1 = 4;
	//const int myDIM2 = 4;
	
	if (offset1 + readDim1 > myDIM1) {
	  std::cout << "offset (" << offset1 << ") + read-dimension (" <<
			readDim1 << ") must be less than array dim1 (" << myDIM1 << ")\n";
		exit(EXIT_FAILURE);
	}
	if (offset2 + readDim2 > myDIM2) {
	  std::cout << "offset (" << offset2 << ") + read-dimension (" <<
			readDim2 << ") must be less than array dim2 (" << myDIM2 << ")\n";
		exit(EXIT_FAILURE);
	}
	read(filename, readDim1, readDim2, offset1, offset2, chunkDim1, chunkDim2);
	return 0;
}
