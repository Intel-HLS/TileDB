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
		printf("check usage\n");
		exit(EXIT_FAILURE);
	}

	char hdf_file_name[1024];
	char data_dir_name[1024];
	strcpy(hdf_file_name, argv[1]);
	const int dim0 = atoi(argv[2]);
	const int dim1 = atoi(argv[3]);
	const int chunkdim0 = atoi(argv[4]);
	const int chunkdim1 = atoi(argv[5]);
	strcpy(data_dir_name, argv[6]);
	int disk_block_size;
	if (argc < 8) {
		disk_block_size = 0;
	} else {
		disk_block_size = atoi(argv[7]);
	}
	int blockcount = (dim0/chunkdim0)*(dim1/chunkdim1);
	size_t buffer_size = chunkdim0 * chunkdim1 * sizeof(int);

	//printf("Creating H5 file: %s\n", hdf_file_name);

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
	filespace = H5Screate_simple(RANK, dimsf, NULL); 

	/*
	 * Create the dataset with default properties and close filespace.
	 */
	dset_id = H5Dcreate(file_id, DATASETNAME, H5T_NATIVE_INT, filespace, 
			H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	H5Sclose(filespace);

	plist_id = H5Dget_create_plist(dset_id);
	H5Pset_deflate(plist_id, 6);
	/* 
	 * Each process defines dataset in memory and writes it to the hyperslab
	 * in the file.
	 */
	int dim0_lo, dim0_hi, dim1_lo, dim1_hi;
	int y = mpi_rank%(dim1/chunkdim1);
	int x = (mpi_rank - y)/(dim1/chunkdim1);
	dim0_lo = x * chunkdim0;
	dim0_hi = dim0_lo + chunkdim0 - 1;
	dim1_lo = y * chunkdim1;
	dim1_hi = dim1_lo + chunkdim1 - 1;
	count[0] = dim0_hi - dim0_lo + 1;
	count[1] = dim1_hi - dim1_lo + 1;
	offset[0] = dim0_lo;
	offset[1] = dim1_lo;
	memspace = H5Screate_simple(RANK, count, NULL);

	//printf("Process: %d running with offset %d %d\n", mpi_rank, offset[0], offset[1]);
	/*
	 * Select hyperslab in the file.
	 */
	filespace = H5Dget_space(dset_id);
	H5Sselect_hyperslab(filespace, H5S_SELECT_SET, offset, NULL, count, NULL);

	/*
	 * Initialize data buffer 
	 */
	char filename[1024];
	int *chunk = (int *) malloc(buffer_size);
	int elements = 0;
	sprintf(filename, "%s/chunk%d.bin", data_dir_name, mpi_rank);
	//printf("Reading file: %s...", filename);
	int fd = open(filename, O_RDONLY);
	if (fd != 0) {
		int bytes = read(fd, (void*)chunk, buffer_size);
	} else printf("Unable to open file: %s\n", filename);
	close(fd);
	elements += buffer_size / sizeof(int);
	//printf("%d elements read completed\n", elements);

	MPI_Barrier(MPI_COMM_WORLD);
	struct timeval g_start, g_end;
	if (mpi_rank==0) { // only if you're the coordinator, start the clock
		gettimeofday(&g_start, NULL);
	}

	/*
	 * Create property list for collective dataset write.
	 */
	plist_id = H5Pcreate(H5P_DATASET_XFER);
	H5Pset_dxpl_mpio(plist_id, H5FD_MPIO_COLLECTIVE);

	struct timeval start, end;
	gettimeofday(&start, NULL);
	status = H5Dwrite(dset_id, H5T_NATIVE_INT, memspace, filespace,
			plist_id, chunk);
	gettimeofday(&end, NULL);
	float time = (float)((end.tv_sec - start.tv_sec)*ONE_MILLION +
								(end.tv_usec - start.tv_usec))/ONE_MILLION;
	//printf("process %d write time: %.3f seconds\n", mpi_rank, time);
	
	free(chunk);

	/*
	 * Close/release resources.
	 */
	H5Dclose(dset_id);
	H5Sclose(filespace);
	H5Sclose(memspace);
	H5Pclose(plist_id);
	H5Fclose(file_id);
	system("sync");

	MPI_Finalize();

	if (mpi_rank==0) {
		gettimeofday(&g_end, NULL);
		float g_time = (float)((g_end.tv_sec - g_start.tv_sec)*ONE_MILLION +
				(g_end.tv_usec - g_start.tv_usec))/ONE_MILLION;

		printf("join after barrier time: %.3f secs\n", g_time);
		gettimeofday(&g_start, NULL);
		system("sync");
		gettimeofday(&g_end, NULL);
		float s_time = (float)((g_end.tv_sec - g_start.tv_sec)*ONE_MILLION +
				(g_end.tv_usec - g_start.tv_usec))/ONE_MILLION;

		printf("sync time: %.3f secs\n", s_time);
		printf("total time: %.3f secs\n", g_time+s_time);
	}
	return 0;
}     
