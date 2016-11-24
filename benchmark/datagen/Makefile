
CONFIG_FLAGS=-D_FILE_OFFSET_BITS=64

LINK_FLAGS=-lstdc++ -lz -lcrypto

# -gdwarf-3 flag is to enable support for older version of gdb. In case there is a
#  mismatch between gcc and gdb versions, symbols are not found in gdb context.
#  Adding this flag solves that problem
CFLAGS=-D_FILE_OFFSET_BITS=64 -g -ggdb3 -Wall -gdwarf-3 -std=c++11
CXX=g++ $(CFLAGS) $(CONFIG_FLAGS) $(INCLUDE_FLAGS)

SRC=$(wildcard *.cc)
SRC+=$(wildcard ais/*.cc)
SRC:=$(filter-out $(OPT_SRC), $(SRC))
PROGS = $(patsubst %.cc,%,$(SRC))

all: $(PROGS) $(OPT_OBJ)
	@echo "All done"

%: %.cc $(OPT_SRC)
	@echo "CXX $<"
	@LD_LIBRARY_PATH=$(HDF5_HOME)/lib:$(LD_LIBRARY_PATH) $(CXX) $(LD_FLAGS) $< -o $@ $(LINK_FLAGS)

clean:
	@rm -rf $(PROGS)
