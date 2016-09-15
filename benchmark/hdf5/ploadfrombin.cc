#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <cstdlib>
#include <sstream>
#include <vector>
#include <chrono>
#include <time.h>
#include <sys/time.h>
#include <pthread.h>

#include "H5Cpp.h"

#ifdef __linux__
#include <sched.h>
#endif

// To compile: h5c++ loaddata.cc

using namespace std;
#ifndef H5_NO_NAMESPACE
    using namespace H5;
#endif

const H5std_string	DATASET_NAME("tiledb_dset");

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

struct mystruct {
	hsize_t start[2];
	DataSet *dataset;
	DataSpace *dataspace;
	char filename[1024];
	int chunkdim1, chunkdim2;
	int dim1, dim2;
	char hdf5file[1024];
};

void *thread_fn(void *args) {
	struct mystruct *myargs = (struct mystruct *)args;
	hsize_t start[2];
	start[0] = myargs->start[0];
	start[1] = myargs->start[1];
	hsize_t count[2];
	count[0] = myargs->chunkdim1;
	count[1] = myargs->chunkdim2;
	//DataSet *dataset = myargs->dataset;
	int chunkdim1 = myargs->chunkdim1;
	int chunkdim2 = myargs->chunkdim2;
	int buffer_size = chunkdim1*chunkdim2*sizeof(int);
	int *buffer = new int [buffer_size];
	cout << buffer_size << "\n";

	//H5File* file = new H5File(myargs->hdf5file, H5F_ACC_TRUNC);
	//DataSet* dataset = new DataSet(file->openDataSet(DATASET_NAME));
	//DataSpace dataspace = dataset->getSpace();
	DataSet *dataset = myargs->dataset;
	DataSpace *dataspace = myargs->dataspace;
	struct timeval endtime, starttime;

	cout << "Thread: " << pthread_self() << ":: input file: " << myargs->filename << "\n";
	cout << "Thread: " << pthread_self() << ":: start: [" << start[0] << "," << start[1] << "]\n";
	try {
		ifstream ifile(myargs->filename, ios::in|ios::binary);
		ifile.seekg(0, ios::beg);
		if (ifile.is_open()) {
			ifile.read((char*)buffer, buffer_size);
			cout << "Thread: " << pthread_self() << ":: file read into buffer\n";
			H5Sselect_hyperslab(dataspace->getId(), H5S_SELECT_SET, start, NULL, count, NULL);
			hsize_t mdim[2] = {count[0], count[1]};
			DataSpace mspace(2, mdim);
			cout << "Thread: " << pthread_self() << ":: just before write\n";
			gettimeofday(&starttime, NULL);
			dataset->write(buffer, PredType::NATIVE_INT, mspace, *dataspace);
			gettimeofday(&endtime, NULL);
			long diff_usecs = (endtime.tv_sec - starttime.tv_sec)*1000000 + (endtime.tv_usec - starttime.tv_usec); 
			cout << "Thread: " << pthread_self() << ":: write wall clock time: " << (float)diff_usecs/1000000 << "\n";
		} else cout << "Thread: " << pthread_self() << ":: Unable to open file: " << myargs->filename << "\n";
		ifile.close();
		//delete[] buffer;
		//dataset->close();
		//file->close();
	}

	// catch failure caused by the H5File operations
	catch(FileIException error)
	{
		error.printError();
		int retval = -1;
		pthread_exit((void*)&retval);
	}

	// catch failure caused by the DataSet operations
	catch(DataSetIException error)
	{
		error.printError();
		int retval = -1;
		pthread_exit((void*)&retval);
	}

	// catch failure caused by the DataSpace operations
	catch(DataSpaceIException error)
	{
		error.printError();
		int retval = -1;
		pthread_exit((void*)&retval);
	}
	return NULL;
}

int main (int argc, char **argv)
{
	if (argc < 8) {
		cerr << "Usage: " << argv[0] << " hdf5-file coreid dim1 dim2 chunkdim1 chunkdim2 chunk-dir\n";
		exit(EXIT_FAILURE);
	}

	const H5std_string	CHUNK_FILE_NAME(argv[1]);
	affinitize(atoi(argv[2]));

	cout << "writing to hdf5: " << argv[1] << " with:\n";
	const int myDIM1 = atoi(argv[3]);
	const int myDIM2 = atoi(argv[4]);
	const int chunkDim1 = atoi(argv[5]);
	const int chunkDim2 = atoi(argv[6]);
	std::string chunkDIR = argv[7];
	cout << "dim0: " << myDIM1 << "\n";
	cout << "dim1: " << myDIM2 << "\n";
	cout << "chunk0: " << chunkDim1 << "\n";
	cout << "chunk1: " << chunkDim2 << "\n";
	//long blockcount = ((long)myDIM1*myDIM2)/((long)chunkDim1*chunkDim2);
	int blockcount = 2;
	cout << "Blockcount: " << blockcount << "\n"; 

	try {
		H5File *file = new H5File(CHUNK_FILE_NAME, H5F_ACC_TRUNC);
		DSetCreatPropList plist;
		hsize_t chunk_dims[2];
		chunk_dims[0] = chunkDim1;
		chunk_dims[1] = chunkDim2;
		plist.setChunk(2, chunk_dims);
		plist.setAllocTime(H5D_ALLOC_TIME_INCR);

		hsize_t fdim[] = {myDIM1, myDIM2};
		DataSpace dataspace(2, fdim);
		struct timeval endtime, starttime;
		gettimeofday(&starttime, NULL);
		DataSet* dataset = new DataSet(file->createDataSet(DATASET_NAME, PredType::NATIVE_INT, dataspace, plist));
		gettimeofday(&endtime, NULL);
		std::cout << "Init time: " << (float)((endtime.tv_sec - starttime.tv_sec)*1000000 +
				(endtime.tv_usec - starttime.tv_usec))/1000000 << " secs\n";
		//dataset->close();
		//file->close();

		hsize_t start[2];
		hsize_t count[2];
		count[0] = chunkDim1;
		count[1] = chunkDim2;
		start[0] = 0;
		start[1] = 0;

		pthread_t tid[blockcount];
		gettimeofday(&starttime, NULL);
		char filename[1024];
		int buffer_size = chunkDim1*chunkDim2*sizeof(int);
		for (int i = 0; i < blockcount; ++i) {
			sprintf(filename, "%s/chunk%d.bin", chunkDIR.c_str(), i);
			int y = i%(myDIM2/chunkDim2);
			int x = (i - y)/(myDIM2/chunkDim2);
			start[0] = x * chunkDim1;
			start[1] = y * chunkDim2;
			std::cout << "Start: [" << start[0] << "," << start[1] << "]\n";
			struct mystruct *args = new struct mystruct();
			args->start[0] = start[0];
			args->start[1] = start[1];
			args->chunkdim1 = chunkDim1;
			args->chunkdim2 = chunkDim2;
			args->dataset = dataset;
			args->dim1 = myDIM1;
			args->dim2 = myDIM2;
			args->dataset = dataset;
			args->dataspace = &dataspace;
			strcpy(args->filename, filename);
			strcpy(args->hdf5file, CHUNK_FILE_NAME.c_str());
			pthread_create(&tid[i], NULL, thread_fn, args);
		}	// end of for

		for (int i = 0; i < blockcount; ++i) {
			pthread_join(tid[i], NULL);
		}

		gettimeofday(&endtime, NULL);
		long diff_usecs = (endtime.tv_sec - starttime.tv_sec)*1000000 + (endtime.tv_usec - starttime.tv_usec); 
		std::cout << "total write wall clock time: " << (float)diff_usecs/1000000 << " s\n";
		gettimeofday(&starttime, NULL);
		//dataset->close();
		//file->close();
		delete dataset;
		delete file;
		system("sync");
		gettimeofday(&endtime, NULL);
		float finalize_time = (float)((endtime.tv_sec - starttime.tv_sec)*1000000 + (endtime.tv_usec - starttime.tv_usec))/1000000;
		std:cout << "finalize time: " << finalize_time << " secs\n";
	}

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
}
