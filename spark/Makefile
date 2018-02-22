##########
# Macros #
##########

PROJECT = tiledb-spark

DYNLIB = libTileDB.so

OS := $(shell uname)

# Large file support
LFS_CFLAGS = -D_FILE_OFFSET_BITS=64

# Parallel sort
GNU_PARALLEL =

ifeq ($(GNU_PARALLEL),)
  GNU_PARALLEL = 1
endif

ifeq ($(GNU_PARALLEL),1)
  CFLAGS = -fopenmp -DGNU_PARALLEL
else
  CFLAGS =
endif

# --- Compilers --- #
ifndef JAVA_HOME
$(error JAVA_HOME must be set.)
endif
JAVAEXT=linux

ifndef TILEDB_HOME
$(error TILEDB_HOME must be set.)
endif

# Maven for Java/Scala build
MVN=mvn
MVN_OPTIONS=-DskipTests -q

# Compiler for C++/MPI
CFLAGS = -fmax-errors=5 -D_GLIBCXX_USE_NANOSLEEP -DNDEBUG -O3

MPIPATH = #/opt/mpich/dev/intel/default/bin/
CXX = $(MPIPATH)mpicxx -lstdc++ -std=c++11 -fPIC -fvisibility=hidden \
      $(LFS_CFLAGS) $(CFLAGS)

# --- Versions --- #
SCALA_VERSION = 2.10
PROJECT_VERSION = 1.0-SNAPSHOT

# --- Command line arguments --- #
ARRAY=KARR2
PARTITION_SIZE=10000000
SPARK_MASTER=spark://localhost:7077

# --- Directories --- #
TILEDB_OBJ_DIRS=$(wildcard $(TILEDB_HOME)/core/obj/release/*)
TILEDB_INCLUDE_DIRS=$(wildcard $(TILEDB_HOME)/core/include/*)
TILEDB_INCLUDE_PATHS=$(addprefix -I, $(TILEDB_INCLUDE_DIRS))

CPP_SRCDIR = src/main/cpp/src
CPP_INCDIR = src/main/cpp/include
CPP_SRC_SUBDIRS = $(wildcard $(CPP_SRCDIR)/*)
SCALA_SRCDIR = src/main/scala/com/intel/tiledb
SCALA_TESTDIR = src/test/scala/com/intel/tiledb
SCALA_SRC_SUBDIRS = $(wildcard $(SCALA_SRCDIR)/*)
SCALA_TEST_SUBDIRS = $(wildcard $(SCALA_TESTDIR)/*)
LIBDIR=lib
OBJDIR = obj

# --- Sources --- #
CPP_SRC = $(wildcard $(foreach D,$(CPP_SRC_SUBDIRS),$D/*.cc))
SCALA_SRC = $(wildcard $(foreach D,$(SCALA_SRC_SUBDIRS),$D/*.scala))
SCALA_SRC += $(wildcard $(SCALA_SRCDIR)/*.scala)
SCALA_TEST_SRC = $(wildcard $(foreach D,$(SCALA_TEST_SUBDIRS),$D/*.scala))
SCALA_TEST_SRC += $(wildcard $(SCALA_TESTDIR)/*.scala)

# --- Object Files --- #
OBJ := $(OBJDIR)/TileDBFactory.o
TILEDB_OBJS=$(wildcard $(foreach D,$(TILEDB_OBJ_DIRS),$D/*.o))


# --- Libs --- #
MPI_LIB = -lmpi
OPENMP_LIB = -fopenmp

# --- Jars --- #
TARGET_JAR = target/$(PROJECT)-$(PROJECT_VERSION)-jar-with-dependencies.jar

all: $(TILEDB_OBJS) $(LIBDIR)/$(DYNLIB)

$(TARGET_JAR): $(SCALA_SRC) $(SCALA_TEST_SRC)
	@echo "[INFO] ------------------------------------------------------------------------"
	@echo "[INFO] Creating $(TARGET_JAR)"
	@$(MVN) $(MVN_OPTIONS) package

# --- Create header files --- #
$(CPP_INCDIR)/TileDBFactory.h: $(TARGET_JAR) $(SCALA_SRCDIR)/TileDBFactory.scala $(CPP_SRC)
	@echo "[INFO] Creating $@"
	@CLASSPATH=$< javah -o $@ com.intel.tiledb.TileDBFactory

# --- Creating TileDBFactory object file --- #
$(OBJ): $(CPP_SRCDIR)/TileDBFactory.cc $(CPP_INCDIR)/TileDBFactory.h
	@echo "[INFO] Creating $@"
	@mkdir -p $(OBJDIR)
	@$(CXX) $(CFLAGS) \
	  -I$(JAVA_HOME)/include/ \
	  -I$(JAVA_HOME)/include/$(JAVAEXT) \
	  -I$(CPP_INCDIR) \
	  $(TILEDB_INCLUDE_PATHS) -c -o $@ $<

# --- Creating TileDBFactory JNI dynamic librayr --- #
$(LIBDIR)/$(DYNLIB): $(CPP_SRC) $(OBJ) $(TILEDB_OBJS) $(TARGET_JAR)
	@echo "[INFO] Creating $@"
	@mkdir -p $(LIBDIR)
	@$(CXX) $(CFLAGS) \
	  -shared -o $@ $< \
	  $(TILEDB_OBJS)
	@echo "[INFO] ------------------------------------------------------------------------"
	@echo "[INFO] BUILD COMPLETE"
	@echo "[INFO] ------------------------------------------------------------------------"

run: all
	@./bin/run.sh $(ARRAY) $(SPARK_MASTER) $(PARTITION_SIZE)

test: all
	@$(MVN) test -Dtiledb.workspace=$(TILEDB_WORKSPACE)

clean:
	find . -type f | xargs -n 5 touch
	rm -rf $(OBJ)
	rm -rf $(LIBDIR)
	rm -rf $(CPP_INCDIR)/TileDBFactory.h
	$(MVN) $(MVN_OPTIONS) clean
