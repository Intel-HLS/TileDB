/* Simple MPI-IO program testing if a parallel file can be created.
 * Default filename can be specified via first program argument.
 * Each process writes something, then reads all data back.
 */

#include <mpi.h>
#ifndef MPI_FILE_NULL           /*MPIO may be defined in mpi.h already       */
#   include <mpio.h>
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <tiledb_tests.h>
#include <fcntl.h>

#define DIMSIZE	10		/* dimension size, avoid powers of 2. */
#define PRINTID printf("Proc %d: ", mpi_rank)

int main(int ac, char **av)
{
	int  mpi_size, mpi_rank;
	MPI_File fh;
	//char *filename = "./mpitest.data";
	char mpi_err_str[MPI_MAX_ERROR_STRING];
	int  mpi_err_strlen;
	int  mpi_err;
	char writedata[DIMSIZE], readdata[DIMSIZE];
	char expect_val;
	int  i, irank; 
	int  nerrors = 0;		/* number of errors */
	MPI_Offset  mpi_off;
	MPI_Status  mpi_stat;

	MPI_Init(&ac, &av);
	MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
	MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

	if (ac < 4) {
		printf("Usage: %s filename ncells srandkey\n", av[0]);
		exit(EXIT_FAILURE);
	}

	char filename[1024];
	strcpy(filename, av[1]);
	const int ncells = atoi(av[2]);
	const int srand_key = atoi(av[3]);

	MPI_Barrier(MPI_COMM_WORLD);
	if (mpi_rank==0) {
		srand(srand_key);
	}

	struct timeval start,end;
	GETTIME(start);

	int posix=0;
	if (posix==0) {
		// Open MPI file
		if ((mpi_err = MPI_File_open(MPI_COMM_WORLD, filename,
						//MPI_MODE_RDWR | MPI_MODE_CREATE | MPI_MODE_DELETE_ON_CLOSE,
						MPI_MODE_RDONLY,
						MPI_INFO_NULL, &fh))
				!= MPI_SUCCESS){
			MPI_Error_string(mpi_err, mpi_err_str, &mpi_err_strlen);
			PRINTID;
			printf("MPI_File_open failed (%s)\n", mpi_err_str);
			return 1;
		}

		// Read 4 bytes from random offsets in the file
		off_t offset, x, count = 0;
		int blocking=1;
		for (i=0;i<ncells;++i) {
			mpi_off = rand() % 1000000 * sizeof(int);

			mpi_err = MPI_File_read_at(fh, mpi_off, &x, sizeof(int), MPI_INT, &mpi_stat);
			if (mpi_err != MPI_SUCCESS) {
				MPI_Error_string(mpi_err, mpi_err_str, &mpi_err_strlen);
				PRINTID;
				printf("MPI_File_read_at offset(%ld), bytes (%d), failed (%s)\n",
						(long) mpi_off, (int) DIMSIZE, mpi_err_str);
				return 1;
			}
			//printf("read data[%d:%d] got %d, expect %d\n", irank, i,
			//		readdata[i], expect_val);
			//nerrors++;
			//count++;
			//printf("%d process read %x at %x\n", mpi_rank, x, mpi_off);
		}
		MPI_File_close(&fh);
	} else {
		for (i=0;i<ncells;++i) {
			int fd = open(filename, O_RDONLY);
			off_t offset = lseek(fd, mpi_off, SEEK_SET);
			int x;
			if (offset!=-1) {
				if (read(fd, &x, sizeof(int)) != sizeof(int)) {
					//cerr << "thread: " << mpi_rank << ":: read error\n";
				}
			}
			close(fd);
		}
	}

	//PRINTID;
	//printf("all tests passed\n");

	MPI_Finalize();

	GETTIME(end);
	printf("%.3f\n", DIFF_TIME_SECS(start, end));
	return 0;
}
