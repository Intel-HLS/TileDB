#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <cstdlib>
#include <sstream>
#include <vector>
#include <map>
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

//const H5std_string	FILE_NAME("/data1/hdf5/tiledb_dset.h5");
const H5std_string	DATASET_NAME("Int32Array");

const int	FSPACE_RANK = 2;          // Dataset rank in file

void writeRandom(const std::string& filename, const int dim0_lo, const int dim0_hi,
									const int dim1_lo, const int dim1_hi, int length, int srand_key) {
	const H5std_string FILE_NAME(filename);
	struct timeval start, end;
	GETTIME(start);
	H5File* file = new H5File(FILE_NAME, H5F_ACC_RDWR);
	// Open the dataset
	DataSet* dataset = new DataSet(file->openDataSet(DATASET_NAME));
	//std::cout << "storage size: " << dataset->getStorageSize() << "\n";
	DataSpace dataSpace = dataset->getSpace();
	GETTIME(end);
	std::cout << "write init wall clock time: " << DIFF_TIME_SECS(start,end) << " secs\n"; 

	//unsigned super, freelist, stab, shhdr;
	//FileCreatPropList plist = file->getCreatePlist();
	//plist.getVersion(super, freelist, stab, shhdr);
	//std::cout << "HDF5 Version: " << super << "," << freelist << "," << stab << "," << shhdr << "\n";

	int r, c, v;
	int buffer[1];

	hsize_t mdim[FSPACE_RANK] = {1,1};
	DataSpace mspace(FSPACE_RANK, mdim);
	hsize_t offset[FSPACE_RANK], count[FSPACE_RANK];
	double secs = 0.0;

	std::map<std::string, int> myMap;
	std::map<std::string, int>::iterator it;
	myMap.clear();

	srand(srand_key);

	for (int kk = 0; kk < 10; ++kk) {
		for (int i = 0; i < length; ++i) {
			std::ostringstream mystream;
			do {
				r = dim0_lo + rand() % (dim0_hi - dim0_lo + 1);
				c = dim1_lo + rand() % (dim1_hi - dim1_lo + 1);
				v = rand() * (-1);
				std::ostringstream mystream;
				mystream << r << "," << c;
				it = myMap.find(mystream.str());
			} while (it!=myMap.end());
			mystream << r << "," << c;
			myMap[mystream.str()] = v;
			buffer[0] = v;
			//std::cout << "(" << r << "," << c << "," << buffer[0] << ")\n";

			offset[0] = r;
			offset[1] = c;
			count[0] = 1;
			count[1] = 1;

			//H5File* file = new H5File(FILE_NAME, H5F_ACC_RDWR);
			//DataSet* dataset = new DataSet(file->openDataSet(DATASET_NAME));
			//DataSpace dataSpace = dataset->getSpace();

			H5Sselect_hyperslab(dataSpace.getId(), H5S_SELECT_SET, offset, NULL, count, NULL);
			GETTIME(start);
			dataset->write(buffer, PredType::NATIVE_INT, mspace, dataSpace);
			//dataset->close();
			//file->close();
			GETTIME(end);

			secs += DIFF_TIME_SECS(start, end);
			//std::cout << " " << secs << "\n";
		}
	}

	GETTIME(start);
	dataset->close();
	file->close();
	delete dataset;
	delete file;
	int ret = system("sync");
	assert(ret!=-1);
	GETTIME(end);

	std::cout << "write wall clock time: " << secs << " secs\n";
	std::cout << "write flush wall clock time: " << DIFF_TIME_SECS(start,end) << " secs\n";	
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
		std::cerr << "Usage: " << argv[0] << " input-hdf5-filename coreid dim0_lo dim0_hi dim1_lo dim1_hi length [srand_key]\n";
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
	const int dim0_lo = atoi(argv[3]);
	const int dim0_hi = atoi(argv[4]);
	const int dim1_lo = atoi(argv[5]);
	const int dim1_hi = atoi(argv[6]);

	const int length = atoi(argv[7]); // 10000;
	int srand_key = (argc < 9) ? 0 : atoi(argv[8]);
	std::cout << "Length: " << length << " Srand key::: " << srand_key << "\n";

	struct timeval start, end;
	GETTIME(start);
	writeRandom(filename, dim0_lo, dim0_hi, dim1_lo, dim1_hi, length, srand_key);
	GETTIME(end);
	std::cout << "whole write time: " << DIFF_TIME_SECS(start,end) << " secs\n";
	return EXIT_SUCCESS;
}
