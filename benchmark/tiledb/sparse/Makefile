
TILEDB_HOME=$(HOME)/TileDB/TileDB
MODE=release
CORE_SRC_DIR = $(TILEDB_HOME)/core/src
CORE_SRC_SUBDIRS = $(wildcard $(TILEDB_HOME)/core/src/*)
CORE_INCLUDE_SUBDIRS=$(wildcard $(TILEDB_HOME)/core/include/*)
CORE_INCLUDE_PATHS=$(addprefix -I, $(CORE_INCLUDE_SUBDIRS))
CORE_OBJ_DIR=$(TILEDB_HOME)/core/obj/$(MODE)
CORE_SRC := $(wildcard $(foreach D,$(CORE_SRC_SUBDIRS),$D/*.cc))
CORE_OBJ := $(patsubst $(CORE_SRC_DIR)/%.cc, $(CORE_OBJ_DIR)/%.o, $(CORE_SRC))
HDF5_HOME=/mnt/app_hdd/kushald/source/hdf5-1.10.0/hdf5
OPENMPI_HOME=/opt/openmpi-1.10

INCLUDE_FLAGS=$(CORE_INCLUDE_PATHS) -I. -I$(OPENMPI_HOME)/include -I$(HDF5_HOME)/include
CONFIG_FLAGS=-D_FILE_OFFSET_BITS=64

LD_FLAGS=-L$(TILEDB_HOME)/core/lib/$(MODE) -L$(HDF5_HOME)/lib
LINK_FLAGS=-lstdc++ -lz -lcrypto -ltiledb -lhdf5_cpp -lhdf5
#CFLAGS=-std=c++11 -fPIC -fvisibility=hidden -fopenmp -D_FILE_OFFSET_BITS=64

# -gdwarf-3 flag is to enable support for older version of gdb. In case there is a
#  mismatch between gcc and gdb versions, symbols are not found in gdb context.
#  Adding this flag solves that problem
CFLAGS=-D_FILE_OFFSET_BITS=64 -g -ggdb3 -Wall -gdwarf-3 -std=c++11
#CXX=/opt/gcc-4.9.1/bin/g++ $(CFLAGS) $(CONFIG_FLAGS) $(LD_FLAGS) $(INCLUDE_FLAGS)
#CXX=$(HDF5_HOME)/bin/h5c++ $(CFLAGS) $(CONFIG_FLAGS) $(LD_FLAGS) $(INCLUDE_FLAGS)
CXX=mpic++ $(CFLAGS) $(CONFIG_FLAGS) $(INCLUDE_FLAGS)

SRC=$(wildcard *.cc)
SRC:=$(filter-out $(OPT_SRC), $(SRC))
PROGS = $(patsubst %.cc,%,$(SRC))

all: $(PROGS) $(OPT_OBJ)
	@echo "All done"

%: %.cc $(OPT_SRC)
	@echo "CXX $<"
	@LD_LIBRARY_PATH=$(HDF5_HOME)/lib:$(LD_LIBRARY_PATH) $(CXX) $(LD_FLAGS) $< -o $@ $(LINK_FLAGS)

clean:
	rm -rf $(PROGS)
