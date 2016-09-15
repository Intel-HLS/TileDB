#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <cstdlib>
#include <sstream>
	#include <vector>
#include <time.h>
#include <sys/time.h>
#include <pthread.h>
#include <mutex>          // std::mutex

#include "H5Cpp.h"

#ifdef __linux__
#include <sched.h>
#endif

// To compile: h5c++ loaddata.cc

using namespace std;
#ifndef H5_NO_NAMESPACE
    using namespace H5;
#endif


std::mutex mtx;           // mutex for critical section

void printMutex(pthread_t tid, float runtime) {
	mtx.lock();
  std::cout << "Thread: " << tid << " took write wall time: " << runtime << " secs\n";
	mtx.unlock();
}


//const H5std_string	FILE_NAME("/data1/hdf5/tiledb_dset.h5");
const H5std_string	DATASET_NAME("tiledb_dset");

const int	FSPACE_RANK = 2;          // Dataset rank in file

int read(const std::string& filename, const int readDim1,
				 const int readDim2, const hsize_t offset1, const hsize_t offset2) {

	const H5std_string FILE_NAME(filename);

	//hid_t       plist_id;
	//plist_id = H5Pcreate(H5P_FILE_ACCESS);
	int sss = readDim1 * readDim2;
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
	H5Pset_cache(plist.getId(), sss * 2, sss * 2, sizeof(int) * sss * 2, 0);
	
	// Check if dataset is chunked.
	int rank_chunk;
	//hsize_t chunk_dims[2] = {2, 100};
	//if (H5D_CHUNKED == cparms.getLayout()) {
		// Get chunking information: rank and dimensions
	//	rank_chunk = cparms.getChunk( 2, chunk_dims);
	//	std::cout << "chunk rank " << rank_chunk << "dimensions "
	//		<< (unsigned long)(chunk_dims[0]) << " x "
	//		<< (unsigned long)(chunk_dims[1]) << std::endl;
	//}

	struct timeval start, end;
	hsize_t stride[FSPACE_RANK], block[FSPACE_RANK];
	hsize_t offset[FSPACE_RANK], count[FSPACE_RANK];
	offset[0] = offset1;
	offset[1] = offset2;
	count[0] = readDim1;
	count[1] = readDim2;
	stride[0] = 1;
	stride[1] = 1;
	block[0] = 1;
	block[1] = 1;

  //int buffer[readDim1][readDim2];
	//int **buffer = (int **) malloc(sizeof(int *) * readDim1);
	//for (int i = 0; i < readDim1; ++i) {
	//  buffer[i] = (int *) malloc(sizeof(int) * readDim2);
	//	for (int j = 0; j < readDim2; ++j) {
	//		buffer[i][j] = 0;
	//	}
	//}
	int *buffer = (int *) malloc(sizeof(int) * readDim1 * readDim2);
	for (int i = 0; i < (readDim1 * readDim2); ++i) {
	  buffer[i] = 0;
	}
	//H5Pset_hypercache(1048576*1024);
  hsize_t memoffset[2] = { 0, 0 };
  DataSpace memSpace(FSPACE_RANK, count);

	// Optimizations
	//H5Pset_cache(plist_id, sss * 2, sss * 2, sizeof(int) * sss * 2, 0);
	//hid_t access_plist_id = H5Fcreate(filename.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, plist_id);
	//hid_t fid = H5Fopen(filename.c_str(), H5F_ACC_RDWR, H5P_DEFAULT);
	//H5Pset_chunk_cache(fid, 400, 2*1000*1000*1000, 0);
	//H5Pclose(plist_id);
	//H5Pclose(fid);


	//dataSpace.selectHyperslab(H5S_SELECT_SET, count, offset, stride, block);
	dataSpace.selectHyperslab(H5S_SELECT_SET, count, offset);
	memSpace.selectHyperslab(H5S_SELECT_SET, count, memoffset);
	clock_t t1, t2;
	gettimeofday(&start, NULL);
	t1 = clock();
	dataset->read(buffer, PredType::NATIVE_INT, memSpace, dataSpace);
	t2 = clock();
	gettimeofday(&end, NULL);
	dataSpace.close();
	dataset->close();

	/*int ** temp_buffer = new int *[chunk_dims[0]];
	for (int i = 0; i < myDIM1; ++i) {
		temp_buffer[i] = new int [chunk_dims[1]];
	}
  hsize_t memoffset[2] = { 0, 0 };
  DataSpace memSpace(FSPACE_RANK, chunk_dims);
	for (int i = offset[0]; i < (offset[0] + readDim1) && i < myDIM1; i += chunk_dims[0]) {
		for (int j = offset[1]; j < (offset[1] + readDim2) && j < myDIM2; j += chunk_dims[1]) {
			dataset->read(temp_buffer, PredType::NATIVE_INT, memSpace, dataSpace);
		}
	}
	*/
	std::cout << "read wall clock time: " << (float)((end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec))/1000000 << " secs\n"; 
	std::cout << "read CPU time: " << (float) (t2 - t1) / CLOCKS_PER_SEC << " secs\n";

  //for (int i = 0; i < readDim1; ++i) {
	//	for (int j = 0; j < readDim2; ++j) {
	//	  std::cout << "buffer[" << i << "][" << j << "]=" << buffer[i][j] << "  ";
	//	}
	//}

  //for (int i = 0; i < (readDim1 * readDim2); ++i) {
	//  std::cout << buffer[i] << " ";
	//}

  int size = readDim1 * readDim2;
	std::cout << buffer[0] << "," << buffer[1] << "," << buffer[2] << "," << buffer[3] << "," << buffer[4] << "\n";
	std::cout << buffer[size - 5] << "," << buffer[size - 4] << "," << buffer[size - 3] << "," << buffer[size - 2] <<
	  "," << buffer[size - 1] << "\n";

	}

void write(const std::string& filename, const int offset1, const int offset2,
		const int writeDim1, const int writeDim2, const int *buffer) {

	const H5std_string FILE_NAME(filename);
	struct timeval start, end;
	gettimeofday(&start, NULL);
	H5File* file = new H5File(FILE_NAME, H5F_ACC_RDWR);
	// Open the dataset
	DataSet* dataset = new DataSet(file->openDataSet(DATASET_NAME));
	//std::cout << "storage size: " << dataset->getStorageSize() << "\n";
	DataSpace dataSpace = dataset->getSpace();
	gettimeofday(&end, NULL);
	std::cout << "write init wall clock time: " <<
		(float)((end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec))/1000000 << " secs\n"; 
	
	hsize_t stride[FSPACE_RANK], block[FSPACE_RANK];
	hsize_t offset[FSPACE_RANK], count[FSPACE_RANK];
	offset[0] = offset1;
	offset[1] = offset2;
	count[0] = writeDim1;
	count[1] = writeDim2;

	hsize_t mdim[FSPACE_RANK] = {(hsize_t)writeDim1,(hsize_t)writeDim2};
	DataSpace mspace(FSPACE_RANK, mdim);

	H5Sselect_hyperslab(dataSpace.getId(), H5S_SELECT_SET, offset, NULL, count, NULL);
	gettimeofday(&start, NULL);
	dataset->write(buffer, PredType::NATIVE_INT, mspace, dataSpace);
	gettimeofday(&end, NULL);

	std::cout << "write wall clock time: " <<
		(float)((end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec))/1000000 << " secs\n"; 

	gettimeofday(&start, NULL);
	dataset->close();
	file->close();
	delete dataset;
	delete file;
	gettimeofday(&end, NULL);

	std::cout << "write flush wall clock time: " <<
		(float)((end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec))/1000000 << " secs\n"; 
}

void writeRandom(const std::string& filename, const int myDIM1, const int myDIM2, const int length) {
	const H5std_string FILE_NAME(filename);
	struct timeval start, end;
	gettimeofday(&start, NULL);
	H5File* file = new H5File(FILE_NAME, H5F_ACC_RDWR);
	// Open the dataset
	DataSet* dataset = new DataSet(file->openDataSet(DATASET_NAME));
	//std::cout << "storage size: " << dataset->getStorageSize() << "\n";
	DataSpace dataSpace = dataset->getSpace();
	gettimeofday(&end, NULL);
	std::cout << "write init wall clock time: " <<
		(float)((end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec))/1000000 << " secs\n"; 

	unsigned super, freelist, stab, shhdr;
	FileCreatPropList plist = file->getCreatePlist();
	plist.getVersion(super, freelist, stab, shhdr);
	std::cout << "HDF5 Version: " << super << "," << freelist << "," << stab << "," << shhdr << "\n";

	int r, c, v;
	int buffer[1];

	hsize_t mdim[FSPACE_RANK] = {1,1};
	DataSpace mspace(FSPACE_RANK, mdim);
	hsize_t stride[FSPACE_RANK], block[FSPACE_RANK];
	hsize_t offset[FSPACE_RANK], count[FSPACE_RANK];
	double secs = 0.0;
	for (int i = 0; i < length; ++i) {
		r = rand() % myDIM1;
		c = rand() % myDIM2;
		v = rand() * (-1);
		buffer[0] = v;
		//std::cout << "new value: " << r << "," << c << "," << buffer[0];

		offset[0] = r;
		offset[1] = c;
		count[0] = 1;
		count[1] = 1;

		//H5File* file = new H5File(FILE_NAME, H5F_ACC_RDWR);
		//DataSet* dataset = new DataSet(file->openDataSet(DATASET_NAME));
		//DataSpace dataSpace = dataset->getSpace();

		H5Sselect_hyperslab(dataSpace.getId(), H5S_SELECT_SET, offset, NULL, count, NULL);
		gettimeofday(&start, NULL);
		dataset->write(buffer, PredType::NATIVE_INT, mspace, dataSpace);
		//dataset->close();
		//file->close();
		gettimeofday(&end, NULL);

		secs += (float)((end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec))/1000000;
		//std::cout << " " << secs << "\n";
	}

	gettimeofday(&start, NULL);
	dataset->close();
	file->close();
	delete dataset;
	delete file;
	gettimeofday(&end, NULL);

	std::cout << "write wall clock time: " << secs << " secs\n";
	std::cout << "write flush wall clock time: " <<
		(float)((end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec))/1000000 << " secs\n";	
}

struct Utils {
	int threadid;
	DataSpace *dataSpace;
	DataSet *dataset;
	int length;
	int myDIM1;
	int myDIM2;
};

std::string filename;

void *threadFn(void *args) {
	struct Utils *utils = (struct Utils *) args;
	const int length = 10000;
	const int myDIM1 = 500000;
	const int myDIM2 = 5000;

	struct timeval start, end;

	gettimeofday(&start, NULL);
	std::ofstream fs("/tmp/temp.t");
	for (int i = 0; i < 1000000; ++i) {
		fs << "Kushal: " << utils->threadid << "," << i << "\n";
	}
	fs.close();
	gettimeofday(&end, NULL);
	exit(0);
	const H5std_string FILE_NAME(filename);
	float sss = (float)((end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec))/1000000;
	printMutex(utils->threadid, sss);
	gettimeofday(&start, NULL);
	H5File* file = new H5File(FILE_NAME, H5F_ACC_RDWR);
	// Open the dataset
	DataSet* dataset = new DataSet(file->openDataSet(DATASET_NAME));
	//std::cout << "storage size: " << dataset->getStorageSize() << "\n";
	DataSpace dataSpace = dataset->getSpace();
	gettimeofday(&end, NULL);
	//std::cout << "\nwrite init wall clock time: " <<
	//	(float)((end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec))/1000000 << " secs" <<
	//	" for thread " << utils->threadid << "\n";
	hsize_t mdim[FSPACE_RANK] = {1,1};
	DataSpace mspace(FSPACE_RANK, mdim);
	hsize_t stride[FSPACE_RANK], block[FSPACE_RANK];
	hsize_t offset[FSPACE_RANK], count[FSPACE_RANK];

	int r, c, v;
	int buffer[1];
	float secs = 0.0;

	for (int i = 0; i < length; ++i) {
		r = rand() % myDIM1;
		c = rand() % myDIM2;
		v = rand() * (-1);
		buffer[0] = v;
		//std::cout << "new value: " << r << "," << c << "," << buffer[0];

		offset[0] = r;
		offset[1] = c;
		count[0] = 1;
		count[1] = 1;

		//H5File* file = new H5File(FILE_NAME, H5F_ACC_RDWR);
		//DataSet* dataset = new DataSet(file->openDataSet(DATASET_NAME));
		//DataSpace dataSpace = dataset->getSpace();

		H5Sselect_hyperslab(dataSpace.getId(), H5S_SELECT_SET, offset, NULL, count, NULL);
		gettimeofday(&start, NULL);
		//utils->dataset->write(buffer, PredType::NATIVE_INT, mspace, (const H5::DataSpace&)*(utils->dataSpace));
		dataset->write(buffer, PredType::NATIVE_INT, mspace, dataSpace);
		//dataset->close();
		//file->close();
		gettimeofday(&end, NULL);

		secs += (float)((end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec))/1000000;
		//std::cout << "Thread: " << utils->threadid << ", time: " << secs << "\n";
	}

	//std::cout << "\nThread: " << utils->threadid << " took total wall time: " << secs << "\n";
	printMutex(utils->threadid, secs);

	gettimeofday(&start, NULL);
	dataset->close();
	file->close();
	delete dataset;
	delete file;
	gettimeofday(&end, NULL);

	secs = (float)((end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec))/1000000;
	printMutex(utils->threadid, secs);
	//std::cout << "\nThread: " << utils->threadid << " write flush wall clock time: " <<
	//	(float)((end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec))/1000000 << " secs\n";

	pthread_exit(NULL);
}

void writeParallel(const std::string& filename, const int myDIM1, const int myDIM2, const int length,
		const int nthreads) {

	const H5std_string FILE_NAME(filename);
	struct timeval start, end;
	//gettimeofday(&start, NULL);
	//H5File* file = new H5File(FILE_NAME, H5F_ACC_RDWR);
	// Open the dataset
	//DataSet* dataset = new DataSet(file->openDataSet(DATASET_NAME));
	//std::cout << "storage size: " << dataset->getStorageSize() << "\n";
	//DataSpace dataSpace = dataset->getSpace();
	//gettimeofday(&end, NULL);
	//std::cout << "write init wall clock time: " <<
	//	(float)((end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec))/1000000 << " secs\n"; 

	int r, c, v;
	int buffer[1];

	pthread_t threads[nthreads];
	int rc = 0;
	for (int i = 0; i < nthreads;++i) {
		struct Utils *u = new struct Utils;
		u->threadid = i;
		u->length = length;
		u->myDIM1 = myDIM1;
		u->myDIM2 = myDIM2;
		rc = pthread_create(&threads[i], NULL, threadFn, (void *)u);
		if (rc){
			std::cout << "Error: unable to create thread," << rc << endl;
			exit(EXIT_FAILURE);
		}
	}

	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	void *status;

	// free attribute and wait for the other threads
	pthread_attr_destroy(&attr);
	for(int i=0; i < nthreads; i++ ){
		rc = pthread_join(threads[i], &status);
		if (rc){
			std::cout << "Error:unable to join," << rc << endl;
			exit(EXIT_FAILURE);
		}
		std::cout << "Main: completed thread id :" << i ;
		std::cout << "  exiting with status :" << status << endl;
	}

	pthread_exit(NULL);
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
	struct timeval t1, t2;
	gettimeofday(&t1, NULL);
	if (argc < 5) {
		std::cerr << "Usage: " << argv[0] << " <input-hdf5-filename> <coreid> <offset1> <offset2> <readDim1> <readDim2>\n";
		exit(EXIT_FAILURE);
	}
	filename.assign(argv[1]);
	std::ifstream fs(filename.c_str());
	if (!fs.is_open()) {
		std::cerr << filename << " No such file or directory\n";
		exit(EXIT_FAILURE);		 
	}
	fs.close();

	//affinitize(atoi(argv[2]));
	const int offset1 = atoi(argv[3]);
	const int offset2 = atoi(argv[4]);
	const int readDim1 = atoi(argv[5]);
	const int readDim2 = atoi(argv[6]);

	std::cout << "Running with readDim1: " << readDim1 << ", readDim2: " << readDim2 << 
	  " offset1: " << offset1 << " offset2: " << offset2 << "\n";
	//const int myDIM1 = 16;
	//const int myDIM2 = 55377408;
	//const int myDIM1 = 4;
	//const int myDIM2 = 4;
	

	int *buffer = new int [readDim1 * readDim2];
	for (int i = 0; i < (readDim1*readDim2); ++i) {
		buffer[i] = (-1) * i;
	}
	std::cout << "Writing to offset1: " << offset1 << " offset2: " << offset2 <<
		" writeDim1: " << readDim1 << " writeDim2: " << readDim2 << "\n";

	const int myDIM1 = 500000;
	const int myDIM2 = 5000;
	const int length = 10000;

	int rc=0;
	struct timeval start, end;
	gettimeofday(&start, NULL);
	//write(filename, offset1, offset2, readDim1, readDim2, buffer);
	//writeRandom(filename, myDIM1, myDIM2, length);
	writeParallel(filename, myDIM1, myDIM2, length, atoi(argv[7]));
	gettimeofday(&end, NULL);
	std::cout << "whole write time: " << 
		(float)((end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec))/1000000 << " secs\n"; 
	rc=system("/home/user/workspace/clean_caches.sh");
	rc=system("/home/user/workspace/clean_caches.sh");
	struct timeval rstart, rend;
	gettimeofday(&rstart, NULL);
	rc=system("/home/user/workspace/clean_caches.sh");
	rc=system("/home/user/workspace/clean_caches.sh");
	read(filename, readDim1, readDim2, offset1, offset2);
	gettimeofday(&rend, NULL);
	std::cout << "whole read time: " << 
		(float)((rend.tv_sec - rstart.tv_sec) * 1000000 + (rend.tv_usec - rstart.tv_usec))/1000000 << " secs\n"; 
	return 0;
}
