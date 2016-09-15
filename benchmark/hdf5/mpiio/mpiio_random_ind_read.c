#include <tiledb_tests.h>
#include <assert.h>
#include "hdf5.h"
#include <string.h>
#include <stdlib.h>

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

int main(int argc, char **argv) {

	hid_t fid1;			/* HDF5 file IDs */
	hid_t acc_tpl1;		/* File access templates */
	hid_t file_dataspace;	/* File dataspace ID */
	hid_t mem_dataspace;	/* memory dataspace ID */
	hid_t dataset1;	/* Dataset ID */
	DATATYPE data_array1[SPACE1_DIM1][SPACE1_DIM2];	/* data buffer */
	DATATYPE data_origin1[SPACE1_DIM1][SPACE1_DIM2];	/* expected data buffer */

	herr_t ret;         	/* Generic return value */
	int i,j;

	MPI_Init(&argc,&argv);
	MPI_Comm comm = MPI_COMM_WORLD;
	MPI_Info info = MPI_INFO_NULL;
	MPI_Comm_size(comm, &mpi_size);
	MPI_Comm_rank(comm, &mpi_rank);

	if (argc < 7) {
		if (mpi_rank==0) {
			printf("Usage: %s hdf5-filename dim0 dim1 ncells randfilename verify\n", argv[0]);
		}
		exit(EXIT_FAILURE);
	}

	char filename[1024];
	strcpy(filename, argv[1]);
	const int64_t dim0 = atoll(argv[2]);
	const int64_t dim1 = atoll(argv[3]);
	const int ncells = atoi(argv[4]);
	char rfilename[1024];
	strcpy(rfilename, argv[5]);
	const int verify = (argc==7) ? atoi(argv[6]) : 0;

	uint64_t r, c, v;
	uint64_t	*buffer_dim0 = (uint64_t *) malloc(ncells * sizeof(uint64_t));
	uint64_t	*buffer_dim1 = (uint64_t *) malloc(ncells * sizeof(uint64_t));

	int cells_per_process = ncells/mpi_size;
	hsize_t *coord_buffer = (hsize_t *) malloc(sizeof(hsize_t)*RANK*ncells);

	int chunkdim0 = dim0; // 1000;
	int chunkdim1 = dim1; // 1000;
	int index = 0;

	struct timeval g_start, g_end;
	GETTIME(g_start);

	FILE *fp = fopen(rfilename,"r");
	if (fp==NULL) {
		printf("file open error\n");
		exit(EXIT_FAILURE);
	}
	int id = 0;
	while(fscanf(fp,"%d %ld %ld",&id,&r,&c)!=EOF) {
		buffer_dim0[index] = r;
		buffer_dim1[index] = c;
		index++;
	}
	fclose(fp);

	if (index != ncells) {
		printf("error: number of cell doesn't match with lines in file: %d/%d\n", index, ncells);
		exit(EXIT_FAILURE);
	}

	index=0;
	int line_offset = mpi_rank*cells_per_process;
	for (i = line_offset; i < line_offset+cells_per_process; ++i) {
		coord_buffer[2*index] = buffer_dim0[i];
		coord_buffer[2*index+1] = buffer_dim1[i];
		index++;
	}	

	/* setup file access template */
	acc_tpl1 = H5Pcreate (H5P_FILE_ACCESS);
	assert(acc_tpl1 != FAIL);
	H5Pset_cache(acc_tpl1, 0, 400000, 8000000000, 0.75);
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
	size_t nslots, nbytes;
	double w0;
	//H5Pget_chunk_cache(dapl_id, &nslots, &nbytes, &w0);
	//printf("Before: %ld %ld %f\n", nslots, nbytes, w0);
	H5Pset_chunk_cache(dapl_id, 1000*400, 8000000000, 0.75);	
	//H5Pget_chunk_cache(dapl_id, &nslots, &nbytes, &w0);
	//printf("After: %ld %ld %f\n", nslots, nbytes, w0);

	hid_t fapl_id = H5Fget_access_plist(fid1);
	int mdc_nelmts;
	size_t rdcc_nelmts;
	size_t rdcc_nbytes;
	double rdcc_w0;
	H5Pget_cache(fapl_id, &mdc_nelmts, &rdcc_nelmts, &rdcc_nbytes, &rdcc_w0);
	//printf("file access plist: %d %d %d %f\n", mdc_nelmts, rdcc_nelmts, rdcc_nbytes, rdcc_w0);

	/* open the dataset1 collectively */
	dataset1 = H5Dopen2(fid1, DATASETNAME, dapl_id);
	assert(dataset1 != FAIL);

	//hid_t dcpl_id = H5Dget_create_plist(dataset1);
	//hsize_t cdims[2];
	//H5Pget_chunk(dcpl_id, 2, cdims);
	//printf("chunks: %d %d\n", cdims[0], cdims[1]);

	/* create a file dataspace independently */
	file_dataspace = H5Dget_space (dataset1);
	assert(file_dataspace != FAIL);
	ret = H5Sselect_elements(file_dataspace, H5S_SELECT_SET, cells_per_process, (const hsize_t*)coord_buffer);
	assert(ret != FAIL);

	/* create a memory dataspace independently */
	hsize_t dims[]={cells_per_process};
	mem_dataspace = H5Screate_simple(1,dims, NULL);	

	int *buffer = (int *) malloc(sizeof(int) * ncells);

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
	MPI_Finalize();

	GETTIME(g_end);
	if (mpi_rank==0) {
		printf("%.3f\n", DIFF_TIME_SECS(g_start, g_end));
	}

	if (verify==1) {
		printf("verifying...\n");
		for (i=0;i<ncells;i++) {
			if (buffer[i] != coord_buffer[2*i]*dim1+coord_buffer[2*i+1]) {
				printf("no match\n");
				exit(EXIT_FAILURE);
			}
			//printf("%d==[%d,%d]\n", buffer[i], coord_buffer[2*i], coord_buffer[2*i+1]);
			printf("%ld %ld %d\n", coord_buffer[2*i], coord_buffer[2*i+1], buffer[i]);
		}
	}
	return EXIT_SUCCESS;
}
