/*  
 *  This example writes data to the HDF5 file by rows.
 *  Number of processes is assumed to be 1 or multiples of 2 (up to 8)
 */
 
#include "hdf5.h"
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <tiledb_tests.h>

#define FAIL -1

int
main (int argc, char **argv)
{
	/*
	 * HDF5 APIs definitions
	 */ 	
	hid_t       file_id, dset_id;         /* file and dataset identifiers */
	hid_t       filespace, memspace;      /* file and memory dataspace identifiers */
	hsize_t     dimsf[2];                 /* dataset dimensions */
	int         *data;                    /* pointer to data buffer to write */
	hsize_t	count[2];	          /* hyperslab selection parameters */
	hsize_t	offset[2];
	hsize_t stride[2];
	hid_t	plist_id;                 /* property list identifier */
	int         i;
	herr_t	status;

	/*
	 * MPI variables
	 */
	int mpi_size, mpi_rank;
	MPI_Comm comm  = MPI_COMM_WORLD;
	MPI_Info info  = MPI_INFO_NULL;

	/*
	 * Initialize MPI
	 */
	MPI_Init(&argc, &argv);
	MPI_Comm_size(comm, &mpi_size);
	MPI_Comm_rank(comm, &mpi_rank);

	if (argc < 7) {
		printf("Usage: %s hdffile dim0 dim1 chunkdim0 chunkdim1 datadir [diskblocksize]\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	char hdf_file_name[1024];
	char data_dir_name[1024];
	strcpy(hdf_file_name, argv[1]);
	const int64_t dim0 = atoll(argv[2]);
	const int64_t dim1 = atoll(argv[3]);
	const int64_t chunkdim0 = atoll(argv[4]);
	const int64_t chunkdim1 = atoll(argv[5]);
	strcpy(data_dir_name, argv[6]);
	int disk_block_size;
	if (argc < 8) {
		disk_block_size = 0;
	} else {
		disk_block_size = atoi(argv[7]);
	}
	int blockcount = (dim0/chunkdim0)*(dim1/chunkdim1);
	size_t chunk_byte_size = chunkdim0 * chunkdim1 * sizeof(int);
	int nchunks_per_process = blockcount/mpi_size;

	if (mpi_rank==0) {
		printf("Chunks/proc=%d\n", nchunks_per_process);
	}

	//MPI_Barrier(MPI_COMM_WORLD);
	struct timeval g_start, g_end, starttime, endtime;
	GETTIME(g_start);

	/* 
	 * Set up file access property list with parallel I/O access
	 */
	plist_id = H5Pcreate(H5P_FILE_ACCESS);
	H5Pset_fapl_mpio(plist_id, comm, info);

	/*
	 * Create a new file collectively and release property list identifier.
	 */
	file_id = H5Fcreate(hdf_file_name, H5F_ACC_TRUNC, H5P_DEFAULT, plist_id);
	H5Pclose(plist_id);

	/*
	 * Create the dataspace for the dataset.
	 */
	dimsf[0] = dim0;
	dimsf[1] = dim1;
	filespace = H5Screate_simple(2, dimsf, NULL); 

	/*
	 * Create the dataset creation property lits
	 */
	hid_t dcpl_id = H5Pcreate(H5P_DATASET_CREATE);
	assert(dcpl_id!=-1);
	hsize_t chunkdims[2] = {chunkdim0, chunkdim1};
	H5Pset_chunk(dcpl_id, 2, chunkdims);
	H5Pset_alloc_time(dcpl_id, H5D_ALLOC_TIME_INCR);
	//H5Pset_deflate(dcpl_id, 6); -- not supported in PHDF5 - 1.10.0

	/*
	 * Create the dataset with default properties and close filespace.
	 */
	dset_id = H5Dcreate(file_id, DATASETNAME, H5T_NATIVE_INT, filespace,
			H5P_DEFAULT, dcpl_id, H5P_DEFAULT);
	H5Pclose(dcpl_id);
	H5Sclose(filespace);

	//exit(0);
	/*
	 * Initialize data buffer 
	 */
	GETTIME(starttime);
	int **chunks = (int **) malloc(sizeof(int*)*nchunks_per_process);
	char filename[1024];
	int elements = 0;
	int chunkid = 0;
	for (i=0;i<nchunks_per_process;++i) {
		chunkid = mpi_rank*nchunks_per_process+i;
		chunks[i] = (int *) malloc(chunk_byte_size);
		sprintf(filename, "%s/chunk%d.bin", data_dir_name, chunkid);
		//printf("Reading file: %s...", filename);
		int fd = open(filename, O_RDONLY);
		if (fd != -1) {
			int bytes = read(fd, (void*)chunks[i], chunk_byte_size);
		} else {
			printf("Unable to open file: %s\n", filename);
			MPI_Abort(comm, EXIT_FAILURE);
		}
		close(fd);
		elements += chunk_byte_size / sizeof(int);
		//printf("%d elements read completed\n", elements);
	}
	GETTIME(endtime);
	//printf("read time: %.3f\n", DIFF_TIME_SECS(starttime,endtime));

	int dim0_lo, dim0_hi, dim1_lo, dim1_hi;

	for (i=0;i<nchunks_per_process;++i) {

		int y = (mpi_rank*nchunks_per_process+i)%(dim1/chunkdim1);
		int x = ((mpi_rank*nchunks_per_process+i) - y)/(dim1/chunkdim1);
		dim0_lo = x * chunkdim0;
		dim0_hi = dim0_lo + chunkdim0 - 1;
		dim1_lo = y * chunkdim1;
		dim1_hi = dim1_lo + chunkdim1 - 1;
		//if (i==0) {
		offset[0] = dim0_lo;
		offset[1] = dim1_lo;
		//}
		//}
		//count[0] = dim0_hi-offset[0]+1;
		//count[1] = dim1_hi-offset[1]+1;
		count[0] = dim0_hi-dim0_lo+1;
		count[1] = dim1_hi-dim1_lo+1;
		//printf("Read: start[%d,%d] and count[%d,%d]\n", offset[0], offset[1], count[0], count[1]);
		stride[0] = 1;
		stride[1] =1;

		/*
		 * Select hyperslab in the file.
		 */
		filespace = H5Dget_space(dset_id);
		H5Sselect_hyperslab(filespace, H5S_SELECT_SET, offset, stride, count, NULL);

		/* create a memory dataspace independently */
		hsize_t mcount[] = {nchunks_per_process,chunk_byte_size/sizeof(int)};
		memspace = H5Screate_simple (2, count, NULL);
		assert (memspace != FAIL);

		/*
		 * Create property list for collective dataset write.
		 */
		plist_id = H5Pcreate(H5P_DATASET_XFER);
		H5Pset_dxpl_mpio(plist_id, H5FD_MPIO_COLLECTIVE);

		struct timeval start, end;
		GETTIME(start);
		status = H5Dwrite(dset_id, H5T_NATIVE_INT, memspace, filespace,
				plist_id, chunks[i]);
		GETTIME(end);
		float time = DIFF_TIME_SECS(start, end);
		//printf("process %d write time: %.3f seconds\n", mpi_rank, time);
		H5Sclose(filespace);
		H5Sclose(memspace);
		H5Pclose(plist_id);
	}
	//free(chunks);

	/*
	 * Close/release resources.
	 */
	H5Dclose(dset_id);
	//H5Sclose(filespace);
	//H5Sclose(memspace);
	H5Fclose(file_id);
	//system("sync");

	MPI_Finalize();

	GETTIME(g_end);
	float g_time = DIFF_TIME_SECS(g_start, g_end);

	//printf("join after barrier time: %.3f secs\n", g_time);
	GETTIME(g_start);
	system("sync");
	GETTIME(g_end);
	float s_time = DIFF_TIME_SECS(g_start, g_end);

	//printf("sync time: %.3f secs\n", s_time);
	//printf("total time: %.3f secs\n", g_time+s_time);
	printf("%.3f\n", g_time+s_time);
	return 0;
}
