
#HDF5_HOME = /mnt/app_hdd/kushald/source/hdf5-1.8.16-mpio
HDF5_HOME = /mnt/app_hdd/kushald/source/hdf5-1.10.0-parallel

#INCLUDE_FLAGS = -I/opt/intel/impi/5.0.3.048/intel64/include -I$(HDF5_HOME)/c++/src -I$(HDF5_HOME)/hdf5/include
INCLUDE_FLAGS = -I$(HOME)/workspace/tiledb
#LD_FLAGS = -L$(HDF5_HOME)/hdf5/lib

CFLAGS = -fPIC -fvisibility=hidden -D_FILE_OFFSET_BITS=64 -g
CXX=$(HDF5_HOME)/bin/h5pcc $(CFLAGS)

SRC=mpiio_load_from_bin.c mpiio_random_update.c mpiio_random_ind_read.c  mpiio_chunk_ind_read.c mpiio_load_from_bin_zipped.c mpiio_disk_read_test.c
PROGS = $(patsubst %.c,%,$(SRC))

all: $(PROGS)
	@echo "All done"

%: %.c
	@echo "CXX $<"
	@LD_LIBRARY_PATH=$(HDF5_HOME)/lib:$(LD_LIBRARY_PATH) $(CXX) $(INCLUDE_FLAGS) $< -o $@

clean:
	rm -rf $(PROGS)
