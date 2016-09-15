/*  
 *  This example writes data to the HDF5 file by rows.
 *  Number of processes is assumed to be 1 or multiples of 2 (up to 8)
 */
 
#include "hdf5.h"
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <tiledb_tests.h>

int
main (int argc, char **argv) {
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
	char name[80];
	int proc_name_length;

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
	MPI_Get_processor_name(name,&proc_name_length);

	if (argc < 4) {
		printf("Usage: %s hdffile length datafile\n", argv[0]);
		MPI_Finalize();
		exit(EXIT_FAILURE);
	}

	char hdf_file_name[1024];
	strcpy(hdf_file_name, argv[1]);
	const int length = atoi(argv[2]);
	char datafile[1024];
	strcpy(datafile, argv[3]);

	int *buffer_a1 = (int *) malloc(sizeof(int)*length);
	uint64_t *buffer_dim0 = (uint64_t *) malloc(sizeof(uint64_t)*length);
	uint64_t *buffer_dim1 = (uint64_t *) malloc(sizeof(uint64_t)*length);

	struct timeval g_start, g_end;

	GETTIME(g_start);
	FILE *fp = fopen(datafile,"r");
	if (!fp) {
		printf("unable to open file %s\n", datafile);
		MPI_Abort(comm, EXIT_FAILURE);
	}
	uint64_t r,c;
	int v;
	i = 0;
	while (fscanf(fp,"%ld %ld %d",&r,&c,&v)!=EOF) {
		buffer_a1[i] = v * (-1);
		buffer_dim0[i] = r;
		buffer_dim1[i] = c;
		//printf("[%ld,%ld]=%d\n", r,c,v);
		i++;
	}
	fclose(fp);

	int length_per_process = length/mpi_size;
	int *write_buffer = (int *) malloc(sizeof(int)*length_per_process);
	uint64_t *coord_buffer = (uint64_t *) malloc(sizeof(uint64_t)*2*length_per_process);

	int index=0;
	int line_offset = mpi_rank * length_per_process;
	//printf("process: %d :: name : %s :: offset: %d :: length : %d\n",mpi_rank,name,line_offset, length_per_process);
	for (i = line_offset; i < line_offset+length_per_process; ++i) {
		write_buffer[index] = buffer_a1[i] * (-1);
		//printf("[%d,%d]=%d\n", buffer_dim0[i], buffer_dim1[i], buffer_a1[i]);
		coord_buffer[2*index] = buffer_dim0[i];
		coord_buffer[2*index+1] = buffer_dim1[i];
		//printf("process:%d :: [%ld,%ld]=%d\n", mpi_rank,coord_buffer[2*index],
		//		coord_buffer[2*index+1],write_buffer[index]);
		index++;
	}

	plist_id = H5Pcreate(H5P_FILE_ACCESS);
	H5Pset_fapl_mpio(plist_id, comm, info);
	file_id = H5Fopen(hdf_file_name, H5F_ACC_RDWR, plist_id);
	assert(file_id!=-1);
	//H5Pclose(plist_id);
	hid_t dapl_id = H5Pcreate(H5P_DATASET_ACCESS);
	/* set large chunk cache size */
	H5Pset_chunk_cache(dapl_id, 400000, 8000000000, 0.75);
	dset_id = H5Dopen(file_id, DATASETNAME, dapl_id);  
	assert(dset_id!=-1);
	//H5Pclose(dapl_id);
	filespace = H5Dget_space(dset_id);
	count[0] = 1;
	count[1] = 1;

	//memspace = H5Screate_simple(RANK, count, NULL);
	hsize_t dims[]={length_per_process};
	memspace = H5Screate_simple(1,dims, NULL);
	/*
	 * Select hyperslab in the file.
	 */
	//H5Sselect_hyperslab(filespace, H5S_SELECT_SET, offset, NULL, count, NULL);
	H5Sselect_elements(filespace, H5S_SELECT_SET, length_per_process, (const hsize_t*)coord_buffer);
	/*
	 * Create property list for collective dataset write.
	 */
	//plist_id = H5Pcreate(H5P_DATASET_XFER);
	//H5Pset_dxpl_mpio(plist_id, H5FD_MPIO_COLLECTIVE);

	struct timeval start, end;
	GETTIME(start);
	status = H5Dwrite(dset_id, H5T_NATIVE_INT, memspace, filespace,
			H5P_DEFAULT, write_buffer);
	GETTIME(end);
	float time = DIFF_TIME_SECS(start, end);
	//printf("process %d write time: %.3f seconds\n", mpi_rank, time);
	/*
	 * Close/release resources.
	 */
	H5Dclose(dset_id);
	H5Sclose(filespace);
	H5Sclose(memspace);
	H5Pclose(plist_id);
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
	printf("%.3f\n", g_time+s_time);
	return 0;
}
