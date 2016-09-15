
#HDF5_HOME=/mnt/app_hdd/kushald/source/hdf-1.10.0-user/Downloads/hdf5-1.8.16
HDF5_HOME=/mnt/app_hdd/kushald/source/hdf5-1.10.0-linux-centos7-x86_64-gcc485-shared

#INCLUDE_FLAGS = -I/opt/intel/impi/5.0.3.048/intel64/include -I$(HDF5_HOME)/c++/src -I$(HDF5_HOME)/hdf5/include
INCLUDE_FLAGS = -I/opt/intel/impi/5.0.3.048/intel64/include -I/opt/openmpi-1.10/include -I$(HOME)/workspace/tiledb
#LD_FLAGS = -L$(HDF5_HOME)/hdf5/lib

CFLAGS = -std=c++11 -fPIC -fvisibility=hidden -fopenmp -lstdc++ -lpthread -O3
CXX=h5c++ $(CFLAGS)

SRC=$(wildcard *.cc)
PROGS = $(patsubst %.cc,%,$(SRC))

all: $(PROGS)
	@echo "All done"

%: %.cc
	@echo "CXX $<"
	@$(CXX) $(INCLUDE_FLAGS) $< -o $@

clean:
	rm -rf $(PROGS)
