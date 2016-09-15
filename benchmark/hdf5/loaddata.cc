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

const H5std_string	FILE_NAME("/data1/hdf5/tiledb_dset.h5");

const int FSPACE_RANK = 2;          // Dataset rank in file
const int FSPACE_DIM1 = 5;         // dim1 size
const int FSPACE_DIM2 = 10;         // dim2 size
const int MSPACE_RANK = 2;    // Rank of the first dataset in memory
const int MSPACE_DIM1 = 2;    // We will read dataset back from the file
const int MSPACE_DIM2 = 2;    // to the dataset in memory with these

const long SEGMENTSIZE = 1000000;

int ** fileToBuffer(std::ifstream &fs, int myDIM1, int myDIM2) {
  
	std::cout << "Starting..." << "\n";
	int ** buffer = (int **) malloc(sizeof(int *) * myDIM1);
	int linecount = 0;
	char *linestr = new char [100];
	std::vector<int> vals(3);
	vals.clear();
	for (int i = 0; i < myDIM1; ++i) {
		buffer[i] = (int *) malloc(sizeof(int) * myDIM2);
		for (int j = 0; j < myDIM2; ++j) {
			buffer[i][j] = 0;
		}
	}
	std::cout << "Buffer initialized..." << "\n";
		
	for (string line; getline(fs, line);) {
		++linecount;
		strcpy(linestr, line.c_str());
		for (char *p = strtok(linestr, ","); p; p = strtok(NULL, ",")) {
			int x = atoi(p);
			vals.push_back(x);
			if (x %myDIM2 == (myDIM2 - 1)) {
				std::cout << "|";
			}
		}
		buffer[vals[0]][vals[1]] = vals[2];
		vals.clear();
	}
	std::cout << std::endl;
	std::cout << "Elements read: " << linecount << "\n";
	std::cout << "File read into buffer" << "\n";
	return buffer;
}

int writeFromCsv(std::ifstream &fs, const hsize_t myDIM1, const hsize_t myDIM2) {

  std::cout << "Starting to write to HDF5" << std::endl;
	try
	{

		// Turn off the auto-printing when failure occurs so that we can
		// handle the errors appropriately
		//Exception::dontPrint();

		// Create a new file using the default property lists. 
		H5File *file = new H5File(FILE_NAME, H5F_ACC_TRUNC);
		/*
		 * Create property list for a dataset and set up fill values.
		 */
		int fillvalue = 0;   /* Fill value for the dataset */
		DSetCreatPropList plist;
		plist.setFillValue(PredType::NATIVE_INT, &fillvalue);
		hsize_t chunk_dims[FSPACE_RANK];
		chunk_dims[0] = 1;
		chunk_dims[1] = 1000000;
		plist.setChunk(FSPACE_RANK, chunk_dims);

		// Set size of data sieve buffers
		//H5Pset_sieve_buffer_size(plist.getId(), SEGMENTSIZE);
		//H5Pset_deflate(plist.getId(), 0);
		// Create the data space for the dataset in the file
		//const int myDIM2 = 56000000;
		//const int myDIM2 = 84992;
		hsize_t fdim[] = {myDIM1, myDIM2};
		DataSpace dataspace(FSPACE_RANK, fdim);
		// Create the dataset and write it into the file
		auto start_time = chrono::high_resolution_clock::now();
		DataSet* dataset = new DataSet(file->createDataSet(DATASETNAME, PredType::NATIVE_INT, dataspace, plist));
		auto end_time = chrono::high_resolution_clock::now();
		cout << chrono::duration_cast<chrono::microseconds>(end_time - start_time).count() << " usecs\n";

		int *row = new int [SEGMENTSIZE];
		int *col = new int [SEGMENTSIZE];
		int *cell = new int [SEGMENTSIZE];
		vector<int> vals(3);
		long count = 0, linecount = 0, elements = 0;
		char *linestr = new char [100];
		hsize_t start[FSPACE_RANK];
		hsize_t stride[FSPACE_RANK];
		hsize_t arr_count[FSPACE_RANK] = {1,1};
		hsize_t block[FSPACE_RANK];
		long diff_usecs = 0L;
		vals.clear();

		for (string line; getline(fs, line);) {
			++linecount;
			strcpy(linestr, line.c_str());
			for (char *p = strtok(linestr, ","); p; p = strtok(NULL, ",")) {
				int x = atoi(p);
				vals.push_back(x);
				//cout << vals.back() << endl;
				//cout << "size: " << vals.size() << endl;
			}
			row[count] = vals[0];
			col[count] = vals[1];
			cell[count] = vals[2];
			vals.clear();

			//cout << line << "=>" << row[count] << "," << col[count] << "," << cell[count] << "\n";

			if (count == (SEGMENTSIZE - 1) || col[0] + SEGMENTSIZE > (myDIM2 - 1)) {
				start[0] = row[0]; //row[0];
				start[1] = col[0]; //col[0];
				stride[0] = 1;
				stride[1] = count + 1;
				block[0] = 1;
				block[1] = count + 1;
				hsize_t mdim[FSPACE_RANK] = {1,(hsize_t)count+1};
				DataSpace mspace(FSPACE_RANK, mdim);
				auto start_time = chrono::high_resolution_clock::now();
				//std::cout << "i,j,k,count=>" << row[count] << "," << col[count] << "," << cell[count] << "," << count << "\n";
				elements += count + 1;
				//cout << "elements written: " << elements << endl;
				//dataspace.selectHyperslab(H5S_SELECT_SET, arr_count, start, stride, block);
				//dataspace.selectHyperslab(H5S_SELECT_SET, start, NULL, block, NULL);
				H5Sselect_hyperslab(dataspace.getId(), H5S_SELECT_SET, start, NULL, block, NULL);
				dataset->write(cell, PredType::NATIVE_INT, mspace, dataspace);
				auto end_time = chrono::high_resolution_clock::now();
				diff_usecs += chrono::duration_cast<chrono::microseconds>(end_time - start_time).count();
				count = 0;
			} else {
				++count;
			}

		}  // end of for

		if (count > 0) {
			start[0] = row[count];
			start[1] = col[count];
			stride[0] = 1;
			stride[1] = count + 1;
			stride[1] = 1;
			block[0] = 1;
			block[1] = count + 1;
			hsize_t dim1[FSPACE_RANK] = {1, (hsize_t)count+1};
			DataSpace mspace(FSPACE_RANK, dim1);
			auto start_time = chrono::high_resolution_clock::now();
			H5Sselect_hyperslab(dataspace.getId(), H5S_SELECT_SET, start, NULL, block, NULL);
			dataset->write(cell, PredType::NATIVE_INT, mspace, dataspace);
			auto end_time = chrono::high_resolution_clock::now();
			diff_usecs += chrono::duration_cast<chrono::microseconds>(end_time - start_time).count();
			elements += count;
		}
		cout << "Lines parsed: " << linecount << "\n";
		cout << "Elements entered: " << elements << "\n";
		cout << "time required : " << diff_usecs << " usecs\n";
		/*
		 * Get creation properties list.
		 */
		DSetCreatPropList cparms = dataset->getCreatePlist();
		/*
		 * Check if dataset is chunked.
		 */
		int     rank_chunk;
		if (H5D_CHUNKED == cparms.getLayout()) {
			/*
			 * Get chunking information: rank and dimensions
			 */
			rank_chunk = cparms.getChunk( 2, chunk_dims);
			std::cout << "chunk rank " << rank_chunk << "dimensions "
				<< (unsigned long)(chunk_dims[0]) << " x "
				<< (unsigned long)(chunk_dims[1]) << std::endl;
		}
		start_time = chrono::high_resolution_clock::now();
		dataset->close();
		file->close();
		delete dataset;
		delete file;
		end_time = chrono::high_resolution_clock::now();
		cout << "Closing time: " << chrono::duration_cast<chrono::microseconds>(end_time - start_time).count() << "usecs\n";

		auto wr_end_time = chrono::high_resolution_clock::now();
		cout << "Total write time: " <<
			chrono::duration_cast<chrono::microseconds>(wr_end_time - start_time).count() << " usecs\n";
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
}

int writeRowVectorsToMatrix() {
	// Try block to detect exceptions raised by any of the calls inside it
	try
	{
		// Turn off the auto-printing when failure occurs so that we can
		// handle the errors appropriately
		//Exception::dontPrint();

		// Create a new file using the default property lists. 
		H5File *file = new H5File(FILE_NAME, H5F_ACC_TRUNC);
		/*
		 * Create property list for a dataset and set up fill values.
		 */
		int fillvalue = 0;   /* Fill value for the dataset */
		DSetCreatPropList plist;
		//plist.setFillValue(H5T_IEEE_F32LE, &fillvalue);
		plist.setFillValue(PredType::NATIVE_INT, &fillvalue);
		hsize_t chunk_dims[FSPACE_RANK];
		chunk_dims[0] = 5;
		chunk_dims[1] = 3;
		//plist.setChunk(FSPACE_RANK, chunk_dims);
		// Create the data space for the dataset in the file
		hsize_t fdim[] = {FSPACE_DIM1, FSPACE_DIM2};
		DataSpace dataspace(FSPACE_RANK, fdim);

		// Create the dataset and write it into the file
		DataSet* dataset = new DataSet(file->createDataSet(DATASETNAME, PredType::NATIVE_INT, dataspace, plist));

		int buffer[1][FSPACE_DIM2] = {{48, 49, 50, 51, 52, 53, 54, 55, 56, 57}};
		int i = 0, j, k;
		//for (i = 0; i < 1; ++i) {
		//  for (j = 0; j < 10; ++j) {
		//	  buffer[i][j] = 48 + i+j;
		//	}
		//}

    hsize_t dim1[FSPACE_RANK] = {1, FSPACE_DIM2};
    DataSpace mspace(MSPACE_RANK, dim1);

		hsize_t start[FSPACE_RANK];
		hsize_t stride[FSPACE_RANK] = {1,FSPACE_DIM2};
		hsize_t count[FSPACE_RANK] = {1, 1}; // block count
		hsize_t block[FSPACE_RANK] = {1, FSPACE_DIM2}; // block sizes

		for (int i = 0; i < FSPACE_DIM1; ++i) {
			start[0] = i;
			start[1] = 0;
			dataspace.selectHyperslab(H5S_SELECT_SET, count, start, stride, block);
			dataset->write(buffer, PredType::NATIVE_INT, mspace, dataspace);
		}
		/*
		 * Get creation properties list.
		 */
		DSetCreatPropList cparms = dataset->getCreatePlist();
		/*
		 * Check if dataset is chunked.
		 */
		int rank_chunk;
		if (H5D_CHUNKED == cparms.getLayout()) {
			/*
			 * Get chunking information: rank and dimensions
			 */
			rank_chunk = cparms.getChunk( 2, chunk_dims);
			std::cout << "chunk rank " << rank_chunk << "dimensions "
				<< (unsigned long)(chunk_dims[0]) << " x "
				<< (unsigned long)(chunk_dims[1]) << std::endl;
		}
		dataset->close();
		file->close();
		delete dataset;
		delete file;
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
}

int writeBufferToHDF5(int **&buffer, std::string &filename, const hsize_t myDIM1, const hsize_t myDIM2,
											int chunkDim1, int chunkDim2, const int compress) {

	const H5std_string	CHUNK_FILE_NAME(filename.c_str());
	std::cout << "HDF5 Filename: " << filename << "\n";

	try
	{
		H5File *file = new H5File(CHUNK_FILE_NAME, H5F_ACC_TRUNC);
		DSetCreatPropList plist;
		hsize_t chunk_dims[FSPACE_RANK];
		chunk_dims[0] = chunkDim1;
		chunk_dims[1] = chunkDim2;
		plist.setChunk(FSPACE_RANK, chunk_dims);
		if (compress == 1) {
			plist.setDeflate(6);
		}
		plist.setAllocTime(H5D_ALLOC_TIME_INCR);

		hsize_t start[2];
		hsize_t block[2];
		hsize_t stride[2];
		hsize_t count[2];
		long diff_usecs = 0L;
		long diff_clock = 0L;

		int blocks = 0;
		long elements = 0L;
		int cell_index = 0;
		hsize_t fdim[] = {myDIM1, myDIM2};
		DataSpace dataspace(FSPACE_RANK, fdim);
		struct timeval endtime, starttime;
		gettimeofday(&starttime, NULL);
		DataSet* dataset = new DataSet(file->createDataSet(DATASETNAME, PredType::NATIVE_INT, dataspace, plist));
		gettimeofday(&endtime, NULL);
		std::cout << "Init time: " << (float)((endtime.tv_sec - starttime.tv_sec)*1000000 +
			(endtime.tv_usec - starttime.tv_usec))/1000000 << " secs\n";

		clock_t t1, t2;
		int *cells = (int *) malloc(sizeof(int) * chunkDim1 * chunkDim2);
		for (int i = 0; i < myDIM1; i += chunkDim1) {
			for (int j = 0; j < myDIM2; j += chunkDim2) {
				long tile_rows = ((i + chunkDim1) < myDIM1) ? chunkDim1 : (myDIM1 - i);
				long tile_cols = ((j + chunkDim2) < myDIM2) ? chunkDim2 : (myDIM2 - j);
				int k, l;
				//for (int a = 0; a < (chunk_dims[0] * chunk_dims[1]); ++a) cells[a] = 0;
				for (k = 0; k < tile_rows; ++k) {
					for (l = 0; l < tile_cols; ++l) {
						cells[cell_index] = buffer[i + k][j + l];
						cell_index++;
					}
				} // end of k-l loop
				cell_index = 0;
				blocks += 1;
				hsize_t mdim[FSPACE_RANK] = {(hsize_t)k,(hsize_t)l}; // chunk_dims[0], chunk_dims[1]};
				DataSpace mspace(FSPACE_RANK, mdim);
				start[0] = 0;
				start[1] = 0;
				block[0] = 1;
				block[1] = k * l; // chunk_dims[0] * chunk_dims[1];
				//H5Sselect_hyperslab(mspace.getId(), H5S_SELECT_SET, start, NULL, block, NULL);
				start[0] = i;
				start[1] = j;
				block[0] = k; // chunk_dims[0];
				block[1] = l; // chunk_dims[1];
				H5Sselect_hyperslab(dataspace.getId(), H5S_SELECT_SET, start, NULL, block, NULL);
				t1 = clock();
				gettimeofday(&starttime, NULL);
				dataset->write(cells, PredType::NATIVE_INT, mspace, dataspace);
				//std::cout << "called write with i:" << i << " j:" << j << " writesize: " << (k*l*sizeof(int)) << "\n";
				//H5DOwrite_chunk(dataset->getId(), plist.getId(), 0, start, k*l, cells);
				gettimeofday(&endtime, NULL);
				t2 = clock();
				diff_usecs += (endtime.tv_sec - starttime.tv_sec)*1000000 + (endtime.tv_usec - starttime.tv_usec); 
				diff_clock += (t2 - t1);
				elements += tile_rows * tile_cols; // (chunk_dims[0] * chunk_dims[1]);
				//std::cout << "Blocks: " << blocks << " ";
				//std::cout << "Elements written: " << elements << std::endl;
			} // end of j-block
			//std::cout << blocks << "\n";
		}

		std::cout << "\nBlocks written: " << blocks << "\n";
		std::cout << "Elements written: " << elements << "\n";
		std::cout << "Write call time: " << (float)diff_usecs/1000000 << " secs\n";
		std::cout << "Write CPU time: " << (float) (diff_clock / CLOCKS_PER_SEC) << " secs\n";

		/*
		 * Get creation properties list.
		 */
		DSetCreatPropList cparms = dataset->getCreatePlist();
		/*
		 * Check if dataset is chunked.
		 */
		int rank_chunk;
		if (H5D_CHUNKED == cparms.getLayout()) {
			/*
			 * Get chunking information: rank and dimensions
			 */
			rank_chunk = cparms.getChunk( 2, chunk_dims);
			std::cout << "chunk rank " << rank_chunk << "dimensions "
				<< (unsigned long)(chunk_dims[0]) << " x "
				<< (unsigned long)(chunk_dims[1]) << std::endl;
		}
		gettimeofday(&starttime, NULL);
		dataset->close();
		file->close();
		delete dataset;
		delete file;
		system("sync");
		gettimeofday(&endtime, NULL);
		float finalize_time = (float)((endtime.tv_sec - starttime.tv_sec)*1000000 + (endtime.tv_usec - starttime.tv_usec))/1000000;
		std:cout << "finalize time: " << finalize_time << " secs\n";

		std::cout << "total write time: " << (finalize_time + (float)diff_usecs/1000000) << " secs\n";
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

int ** generateBuffer(const int myDIM1, const int myDIM2) {
	int **buffer = (int **) malloc(sizeof(int *) * myDIM1);
	int val = 0;
	for (int i = 0; i < myDIM1; ++i) {
		buffer[i] = (int *) malloc(sizeof(int) * myDIM2);
		for (int j = 0; j < myDIM2; ++j) {
			buffer[i][j] = val++;
			//std::cout << buffer[i][j] << " ";
		}
		if (i != 0 && i % 100000 == 0) {
			std::cout << i << "\n";
		}
	}
	return buffer;
}

int main (int argc, char **argv)
{
	//write();
	//writeRowVectorsToMatrix();
	//read();
	//return 0;

	if (argc < 8) {
		std::cerr << "Usage: " << argv[0] << " <input-filename> <array-name> <coreid> <dim0> <dim1> <chunk_dim0> <chunk_dim1>\n";
		exit(EXIT_FAILURE);
	}
	std::string filename(argv[1]);
	std::ifstream fs(filename.c_str());
	if (!fs.is_open()) {
		std::cerr << filename << " No such file or directory\n";
		exit(EXIT_FAILURE);		 
	}

	affinitize(atoi(argv[3]));
	//const int myDIM1 = 16;
	//const int myDIM2 = 55377408;
	//const int myDIM1 = 10;
	//const int myDIM2 = 20;
	//const int myDIM1 = 360000000;
	//const int myDIM2 = 180000000;
	const int myDIM1 = atoi(argv[4]);
	const int myDIM2 = atoi(argv[5]);

	const int chunkDim1 = atoi(argv[6]);
	const int chunkDim2 = atoi(argv[7]);
	
	//int **buffer = fileToBuffer(fs, myDIM1, myDIM2);
	int **buffer = generateBuffer(myDIM1, myDIM2);

	std::cout << "writing to hdf5\n";
	std::string ofilename = argv[2]; //"/mnt/app_ssd/kushal/hdf5/test_";
	//ofilename += std::to_string(myDIM1);
	//ofilename += "x";
	//ofilename += std::to_string(myDIM2);
	//ofilename += "_";
	//ofilename += std::to_string(chunkDim1);
	//ofilename += "x";
	//ofilename += std::to_string(chunkDim2);
	//ofilename += ".h5";
	writeBufferToHDF5(buffer, ofilename, myDIM1, myDIM2, chunkDim1, chunkDim2, 0);
	//const int myDIM1 = 360000000;
	//const int myDIM2 = 180000000;
	int status = 0;
	//for (int i = 0; i < 10; ++i) {
  	//status = writeFromCsv(fs, myDIM1, myDIM2);
	//}
	if (status != -1) {
	  //read(myDIM1, myDIM2);
	} else {
		cout << "write status: " << status << endl;
	}
	fs.close();
	return 0;
}
