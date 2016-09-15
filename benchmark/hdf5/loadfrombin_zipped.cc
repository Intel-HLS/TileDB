#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <cstdlib>
#include <sstream>
#include <vector>
#include <chrono>
#include <algorithm>
#include <tiledb_tests.h>

#include "H5Cpp.h"

#ifdef __linux__
#include <sched.h>
#endif

// To compile: h5c++ loaddata.cc

using namespace std;
#ifndef H5_NO_NAMESPACE
    using namespace H5;
#endif

int main (int argc, char **argv)
{
	if (argc < 8) {
		cerr << "Usage: " << argv[0] << "hdf5-file dim1 dim2 chunkdim1 chunkdim2 chunk-dir blocksize\n";
		exit(EXIT_FAILURE);
	}

	char hdf_file_name[1024];
	char data_dir_name[1024];
	strcpy(hdf_file_name, argv[1]);
	const uint32_t dim0 = atoi(argv[2]);
	const uint32_t dim1 = atoi(argv[3]);
	const uint32_t chunkdim0 = atoi(argv[4]);
	const uint32_t chunkdim1 = atoi(argv[5]);
	strcpy(data_dir_name, argv[6]);
	size_t block_size = (argc < 8) ? 0 : atoi(argv[7]);
	int blockcount = (dim0/chunkdim0)*(dim1/chunkdim1);
	size_t buffer_size = chunkdim0 * chunkdim1 * sizeof(int);

	cout << "Blockcount: " << blockcount << "\n";

	/* Initialize the buffers from binary files */
	int *chunks[blockcount];
	char filename[1024];

	long elements = 0;
	struct timeval endtime, starttime;
	GETTIME(starttime);
	for (int i = 0; i < blockcount; ++i) {
		sprintf(filename, "%s/chunk%d.bin", data_dir_name, i);
		//cout << "Reading file: " << filename << "...";
		chunks[i] = new int [chunkdim0 * chunkdim1];
		ifstream ifile(filename, ios::in|ios::binary);
		ifile.seekg(0, ios::beg);
		if (ifile.is_open()) {
			ifile.read((char*)chunks[i], buffer_size);
		} //else cout << "Unable to open file: " << filename << "\n";
		ifile.close();
		elements += buffer_size / sizeof(int);
		//cout << elements << " elements read completed\n";
	}
	GETTIME(endtime);
	printf("read time: %.3f\n", DIFF_TIME_SECS(starttime,endtime));

	const uint64_t twogb = 2000000000;
	//const uint64_t dsetsize = dim0*dim1*sizeof(int);
	const uint64_t dsetsize = 0;

	if (dsetsize <= twogb) {
		//int *buffer = new int [dim0*dim1];
		//for (int i = 0; i < blockcount; ++i) {
		//	memcpy((buffer+(i*chunkdim0*chunkdim1)), chunks[i], chunkdim0*chunkdim1*sizeof(int));
		//}
		//sort(buffer, buffer+(dim0*dim1));
		int *buffer2d = new int [dim0*dim1];
		for (int i = 0; i < dim0; ++i) {
			for (int j = 0; j < dim1; ++j) {
				buffer2d[i*dim1+j] = i*dim1+j;
				//cout << buffer2d[i*dim1+j];
			}
		}
		//free(buffer);

		GETTIME(starttime);
		H5File *file = new H5File(hdf_file_name, H5F_ACC_TRUNC);
		DSetCreatPropList plist;
		hsize_t chunk_dims[2] = {chunkdim0, chunkdim1};
		plist.setChunk(2, chunk_dims);
		plist.setAllocTime(H5D_ALLOC_TIME_INCR);
		plist.setDeflate(6);

		hsize_t fdim[] = {(hsize_t)dim0, (hsize_t)dim1};
		DataSpace dataspace(2, fdim);
		DataSet* dataset = new DataSet(file->createDataSet(DATASETNAME, PredType::NATIVE_INT, dataspace, plist));

		hsize_t start[2], count[2];
		start[0] = 0;
		start[1] = 0;
		count[0] = dim0;
		count[1] = dim1;;

		H5Sselect_hyperslab(dataspace.getId(), H5S_SELECT_SET, start, NULL, count, NULL);
		hsize_t mdims[2] = {dim0, dim1};
		DataSpace mspace(2, mdims);
		//hsize_t memstart[2] = {0,0};
		//hsize_t memcount[2] = {dim0, dim1};
		//H5Sselect_hyperslab(mspace.getId(), H5S_SELECT_SET, memstart, NULL, memcount, NULL);
		//DataSpace mspace = H5Screate_simple(2, count, NULL);	
		dataset->write(buffer2d, PredType::NATIVE_INT, mspace, dataspace);
		dataset->close();
		file->close();
		delete dataset;
		delete file;
		system("sync");
		GETTIME(endtime);
		float final_time = DIFF_TIME_SECS(starttime, endtime);
		//cout << "finalize time: " << final_time << " secs\n";
		//cout << "total write wall clock time: " << secs + final_time << " secs\n";
		cout << final_time << "\n";
		delete buffer2d;
		//for(int i = 0; i < blockcount; ++i) delete chunks[i];
		//free(chunks);
		return EXIT_SUCCESS;
	}	// if you can sequentially write all chunks together

	try {
		GETTIME(starttime);
		H5File *file = new H5File(hdf_file_name, H5F_ACC_TRUNC);
		DSetCreatPropList plist;
		hsize_t chunk_dims[2];
		chunk_dims[0] = chunkdim0;
		chunk_dims[1] = chunkdim1;
		plist.setChunk(2, chunk_dims);
		plist.setAllocTime(H5D_ALLOC_TIME_INCR);
		plist.setDeflate(6);

		hsize_t fdim[] = {(hsize_t)dim0, (hsize_t)dim1};
		DataSpace dataspace(2, fdim);
		struct timeval endtime, starttime;
		DataSet* dataset = new DataSet(file->createDataSet(DATASETNAME, PredType::NATIVE_INT, dataspace, plist));
		GETTIME(endtime);
		cout << "Init time: " << DIFF_TIME_SECS(starttime,endtime) << " secs\n";

		hsize_t start[2], block_start[2];
		hsize_t count[2];
		count[0] = chunkdim0;
		count[1] = chunkdim1;
		start[0] = 0;
		start[1] = 0;

		float secs = 0.0;
		for (int i = 0; i < blockcount; ++i) {
			int y = i%(dim1/chunkdim1);
			int x = (i - y)/(dim1/chunkdim1);
			block_start[0] = x * chunkdim0;
			block_start[1] = y * chunkdim1;	
			cout << "Start: [" << block_start[0] << "," << block_start[1] << "]\n";
			if (block_size == 0) {
				H5Sselect_hyperslab(dataspace.getId(), H5S_SELECT_SET, block_start, NULL, count, NULL);
				hsize_t mdims[1] = {count[0] * count[1]};
				DataSpace mspace(1, mdims);
				GETTIME(starttime);
				dataset->write(chunks[i], PredType::NATIVE_INT, mspace, dataspace);
				GETTIME(endtime);
				secs += DIFF_TIME_SECS(starttime,endtime);
				//cout << "Block: " << i << " written\n";
			} else {
				start[0] = block_start[0];
				start[1] = block_start[1];
				size_t residual_write_size = buffer_size;
				for (int j = 0; j < (int)buffer_size; j+=block_size) {
					const int * buffer = (chunks[blockcount] + j/sizeof(int));
					size_t writesize = (residual_write_size < block_size) ? residual_write_size : block_size;
					count[0] = 1;
					count[1] = writesize/sizeof(int);
					H5Sselect_hyperslab(dataspace.getId(), H5S_SELECT_SET, start, NULL, count, NULL);
					if ((start[0]*sizeof(int) + writesize) < chunkdim1) {
						start[1] += writesize/sizeof(int);
					} else if ((start[0]*sizeof(int) + writesize) == chunkdim1) {
						start[0]++;
						start[1] = 0;
					} else {
						cout << "Not handled\n";
						exit(EXIT_FAILURE);
					}
					hsize_t mdims[1] = {writesize};
					DataSpace mspace(1, mdims);
					GETTIME(starttime);
					dataset->write(buffer, PredType::NATIVE_INT, mspace, dataspace);
					GETTIME(endtime);
					secs += DIFF_TIME_SECS(starttime,endtime);	
					residual_write_size -= writesize;
				}	
			}
		}	// end of for
		cout << "write wall clock time: " << secs << " s\n";
		GETTIME(starttime);
		dataset->close();
		file->close();
		delete dataset;
		delete file;
		system("sync");
		GETTIME(endtime);
		float final_time = DIFF_TIME_SECS(starttime, endtime);
		cout << "finalize time: " << final_time << " secs\n";
		printf("%.3f\n", secs + final_time);
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
