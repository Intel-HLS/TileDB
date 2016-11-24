# Data Generator for Benchmarking TileDB

### Dense array
We create a synthetic 2D integer dense array where the coordinates are of type
int64. There is one attribute of type int32. This definition matches the
corresponding array definitions in HDF5 and SciDB so that we match the size of
all IO operations across these systems. To better understand data gen code, we
need to be cognizant of write modes in TileDB - TILEDB_ARRAY_WRITE and
TILEDB_ARRAY_WRITE_SORTED_{COL, ROW}. The former means the user provides buffers
in-order i.e. ordered in the same manner as the buffer will be serialized on
disk. The storage manager will not impose order on the buffers provided. On the
other hand, the storage manager assumes that the provided buffers are in the
global cell order, be it row-major or col-major. Hence, it orders the contents
of the buffer according to tile extents or chunks before writing to disk. Our
first benchmark results (published in VLDB 2017) compares only the former write
method. Hence, the dense array generator writes to disk chunk by chunk. The
chunk size in TileDB are specified by the 'tile extents' in array schema. For
more information on write methods and chunks, please visit the tutorial provided
in http://istc-bigdata.org/tiledb/tutorials/index.html.

To generate an 2D 1000x100 array of dimension ranges 0 to 999 and 0 to 99 with
tile extents 10x10 chunk by chunk, use the generate_dense_array_by_chunks.cc in
the following manner. The script will generate binary files for every chunk of
the dense array in the destination_folder:
```sh
$ generate_dense_array_by_chunks 1000 100 10 10 destination_folder
```

To generate a 2D array of dimension 1000x100 array of dimension ranges 0 to 999
and 0 to 99 with tile extents 10x10 for Vertica(R), use the
generate_dense_array_for_vertica.cc in the following manner. The script will
generate a CSV file for the whole array:
```sh
$ generate_dense_array_for_vertica 1000 100 10 10 0
```
To generate the same 2D array Vertica(R) with the tile id for each cell use the
generate_dense_array_for_vertica.cc in the following manner:
```sh
$ generate_dense_array_for_vertica 1000 100 10 10 1
```

#### Generating random coordinates
To compare random read and random update times, you can use the two scripts
generate_random_coordinates_for_random_reads.cc and
generate_random_coords_and_values_for_random_updates.cc respectively. The first
one will generate random set of coordinates to read from a given 2D array. For
example, to generate 2 random locations for a 2D 1000x100 integer array with
tile extents 10x10, with random seed = 19, use the following command:
```sh
$ generate_random_coordinates_for_random_reads destinationfile 1000 100 2 19 10
10
```
Similarly, to generate 2 random values to be updated to a 2D 1000x100 integer
array with random seed = 19, use the script as:
```sh
$ generate_random_coords_and_values_for_random_updates destinationfiel 1000 100
2 19
```

### Sparse array
Benchmarking of sparse arrays is done on the 2009 and 2010 Automatic
Identification System vessel traffic data. We are unable to share the raw data
as it was provided by our partners. However, the data is publicly available.
Please visit http://marinecadastre.gov/ais/ for information on how to collect
this data. The data comes as CSV files where the schema consists of nine
integers. The first two are (longitude and latitude) of the vessel. Rest are
details on the movement on the vessel. Again, for a detailed description of the
data, please either visit the AIS webpage or consult the TileDB Storage Manager
paper from PVLDB 2017. We scale the coordinates and order them according to
zone, month, longitude and latitude. This ordering is done using some basic
scripting and bash 'sort' command. Once the ordered data is computed, use the
ais/remove_duplicate_entires_from_ais.cc script to remove duplicate entries.
This data can then be used to load to TileDB, SciDB and Vertica.

### License
MIT 2016


