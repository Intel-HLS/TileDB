#include <tiledb_tests.h>
#include <assert.h>
#include "hdf5.h"
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>

#ifdef H5_HAVE_PARALLEL
/* Temporary source code */
#define FAIL -1
/* temporary code end */

/* Define some handy debugging shorthands, routines, ... */
/* debugging tools */
#define MESG(x)\
	if (verbose) printf("%s\n", x);\

#define MPI_BANNER(mesg)\
    {printf("--------------------------------\n");\
    printf("Proc %d: ", mpi_rank); \
    printf("*** %s\n", mesg);\
    printf("--------------------------------\n");}

#define SYNC(comm)\
    {MPI_BANNER("doing a SYNC"); MPI_Barrier(comm); MPI_BANNER("SYNC DONE");}
/* End of Define some handy debugging shorthands, routines, ... */

/* Constants definitions */
/* 24 is a multiple of 2, 3, 4, 6, 8, 12.  Neat for parallel tests. */
#define SPACE1_DIM1	24
#define SPACE1_DIM2	24
#define DATASETNAME1	"Data1"
#define DATASETNAME2	"Data2"
#define DATASETNAME3	"Data3"
/* hyperslab layout styles */
#define BYROW		1	/* divide into slabs of rows */
#define BYCOL		2	/* divide into blocks of columns */

#define PARAPREFIX	"HDF5_PARAPREFIX"	/* file prefix environment variable name */
#endif

/* dataset data type.  Int's can be easily octo dumped. */
typedef int DATATYPE;

/* global variables */
int nerrors = 0;				/* errors count */
#ifndef PATH_MAX
#define PATH_MAX    512
#endif  /* !PATH_MAX */
char    testfiles[2][PATH_MAX];


int mpi_size, mpi_rank;				/* mpi variables */

/* option flags */
int verbose = 0;			/* verbose, default as no. */
int doread=1;				/* read test */
int dowrite=1;				/* write test */
int docleanup=1;			/* cleanup */

void toFile(const char *filename, int *buffer, const size_t size);

int main(int argc, char **argv) {

	hid_t fid1;			/* HDF5 file IDs */
	hid_t acc_tpl1;		/* File access templates */
	hid_t file_dataspace;	/* File dataspace ID */
	hid_t mem_dataspace;	/* memory dataspace ID */
	hid_t dataset1;	/* Dataset ID */
	//DATATYPE data_array1[SPACE1_DIM1][SPACE1_DIM2];	/* data buffer */
	//DATATYPE data_origin1[SPACE1_DIM1][SPACE1_DIM2];	/* expected data buffer */

	hsize_t start[RANK];			/* for hyperslab setting */
	hsize_t count[RANK], stride[RANK];	/* for hyperslab setting */

	herr_t ret;         	/* Generic return value */
	int i,j;

	MPI_Init(&argc,&argv);
	MPI_Comm comm = MPI_COMM_WORLD;
	MPI_Info info = MPI_INFO_NULL;
	MPI_Comm_size(comm, &mpi_size);
	MPI_Comm_rank(comm, &mpi_rank);

	if (argc < 8) {
		if (mpi_rank==0) {
			printf("Usage: %s hdf5-filename dim0 dim1 chunkdim0 chunkdim1 nchunks toFileFlag\n", argv[0]);
		}
		exit(EXIT_FAILURE);
	}

	char filename[1024];
	strcpy(filename, argv[1]);
	const int64_t dim0 = atoll(argv[2]);
	const int64_t dim1 = atoll(argv[3]);
	const int64_t chunkdim0 = atoll(argv[4]);
	const int64_t chunkdim1 = atoll(argv[5]);
	const int nchunks_per_process = atoi(argv[6]);
	const int toFileFlag = atoi(argv[7]);

	struct timeval g_start, g_end;
	int dim0_lo, dim0_hi, dim1_lo, dim1_hi;

	int total_chunks_in_array = (dim0/chunkdim0)*(dim1/chunkdim1);

	//if (mpi_rank==0) {
	GETTIME(g_start);
	//}

	/* setup file access template */
	acc_tpl1 = H5Pcreate (H5P_FILE_ACCESS);
	assert(acc_tpl1 != FAIL);
	/* set Parallel access with communicator */
	ret = H5Pset_fapl_mpio(acc_tpl1, comm, info);
	assert(ret != FAIL);

	/* open the file collectively */
	fid1=H5Fopen(filename,H5F_ACC_RDWR,acc_tpl1);
	assert(fid1 != FAIL);

	/* Release file-access template */
	ret=H5Pclose(acc_tpl1);
	assert(ret != FAIL);

	hid_t dapl_id = H5Pcreate(H5P_DATASET_ACCESS);
	/* set large chunk cache size */
	ret = H5Pset_chunk_cache(dapl_id, total_chunks_in_array*1000, 8000000000, 0.75);

	/* open the dataset1 collectively */
	dataset1 = H5Dopen2(fid1, DATASETNAME, dapl_id);
	assert(dataset1 != FAIL);

	ret = H5Pclose(dapl_id);
	assert(ret != FAIL);

	/* set up dimensions of the slab this process accesses */
	for (i = 0; i < nchunks_per_process; ++i) {
		int y = (mpi_rank*nchunks_per_process+i)%(dim1/chunkdim1);
		int x = ((mpi_rank*nchunks_per_process+i) - y)/(dim1/chunkdim1);
		dim0_lo = x * chunkdim0;
		dim0_hi = dim0_lo + chunkdim0 - 1;
		dim1_lo = y * chunkdim1;
		dim1_hi = dim1_lo + chunkdim1 - 1;
		if (i==0) {
			start[0] = dim0_lo;
			start[1] = dim1_lo;
		}
	}
	count[0] = dim0_hi-start[0]+1;
	count[1] = dim1_hi-start[1]+1;
	//printf("Read: start[%d,%d] and count[%d,%d]\n", start[0], start[1], count[0], count[1]);
	stride[0] = 1;
	stride[1] =1;
	if (verbose)
		printf("start[]=(%lu,%lu), count[]=(%lu,%lu), total datapoints=%lu\n",
				(unsigned long)start[0], (unsigned long)start[1],
				(unsigned long)count[0], (unsigned long)count[1],
				(unsigned long)(count[0]*count[1]));

	/* create a file dataspace independently */
	file_dataspace = H5Dget_space (dataset1);
	assert(file_dataspace != FAIL);
	ret=H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET, start, stride,
			count, NULL);
	assert(ret != FAIL);

	/* create a memory dataspace independently */
	mem_dataspace = H5Screate_simple (RANK, count, NULL);
	assert (mem_dataspace != FAIL);

	int *buffer = (int *) malloc(sizeof(int)*count[0]*count[1]);

	/* read data independently */
	ret = H5Dread(dataset1, H5T_NATIVE_INT, mem_dataspace, file_dataspace,
			H5P_DEFAULT, buffer);
	assert(ret != FAIL);

	/* close dataset collectively */
	ret=H5Dclose(dataset1);
	assert(ret != FAIL);

	/* release all IDs created */
	H5Sclose(file_dataspace);

	/* close the file collectively */
	H5Fclose(fid1);	

	//GETTIME(g_end);
	//printf("Before: %.3f\n", DIFF_TIME_SECS(g_start, g_end)); 

	if (toFileFlag) {
		char filename[1024];
		sprintf(filename, "./tmp/chunk_read_results_chunk%d.bin", mpi_rank);
		printf("writing to file: %s\n", filename);
		toFile(filename, buffer, count[0]*count[1]*sizeof(int));
	}

	free(buffer);
	MPI_Finalize();

	if (mpi_rank==0) {
		GETTIME(g_end);
		printf("%.3f\n", DIFF_TIME_SECS(g_start, g_end)); 
	}
	return EXIT_SUCCESS;
}

void toFile(const char *filename, int *buffer, const size_t size) {
	int fd = open(filename, O_WRONLY | O_APPEND | O_CREAT, S_IRWXU);
	if (write(fd, (void *)buffer, size)==-1) {
		printf("File write error\n");
		exit(EXIT_FAILURE);
	}
	close(fd);
}
