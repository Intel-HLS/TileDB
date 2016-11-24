# TileDB Benchmarking Scripts
This folder contains the scripts to benchmark load, read, update, random read,
parallel load, parallel read parallel update, and consolidation operations in
TileDB.

### Dense Array
In TileDB, we maintain separate scripts for creating, loading and reading data.
In case of dense data, the scripts provided work for a 2D integer dense array.
To create a 2D 1000x100 array with 20x5 tile extents (or chunk size), please use
the dense_create script as shown below. If you want to change the schema, for
example add compression, use this script to recreate the array.
```sh
$ dense_create -a /tmp/MyDenseArray -d 1000,100 -c 20,5
```

Following our example 2D array, to load dense array chunk by chunk, use the
script as shown.  The cells have values = row_id * COLUMNS + col_id. '2' is the
cpu id to which this load process will be affitnized to. The array has 20x5 as
tile extents (or chunk sizes). Hence the chunk ranges provided are 0 to 19 and 0
to 4. The last arguments are array size. We assume the dimensions ranges are
from 0 to (dimension size - 1).
```sh
$ dense_load -a /tmp/MyDenseArray -u 2 -t 0,19,0,4 -d 1000,100
```
To read contiguous subarray or region of the dense array between [100 to 200]
and [0 to 40] using cpu=2, use the script as shown. The last argument lets you
serialize the contents of the buffer read from TileDB. Dump the contents of the
read to a binary file if toFileFlag=1, to a CSV file if toFileFlag=2 and to
stdout if toFileFlag=3. Don't write if toFileFlag=0
```sh
$ dense_read -a /tmp/MyDenseArray -u 2 -t 100,200,0,40 -f 0
```

To read 100 random locations where the coordinates are specified in
/tmp/RandomCoordinates, use the script as shown. Note that the random
coordinates are generated using the script
'datagen/generate_random_coordinates_for_random_reads.cc'
```sh
$ dens_random_read -a /tmp/MyDenseArray -f /tmp/RandomCoordinates -n 100 
```
Two separate scripts are provided to update random values to the dense array in
TileDB and consolidate. Call dense_consolidate as:
```sh
$ dense_consolidate -a /tmp/MyDenseArray
```
To update 100 random locations of the array in a given region, for example [100
to 200] and [0 to 40] with random seed=19, use:
```sh
$ dense_update -a /tmp/MyDenseArray -t 100,200,0,40 -l 100 -r 19
```
#### Parallel operations
For parallel operations on dense arrays, use the scripts provided in the
dense/parallel folder. The naming convenion of the scripts are same as
sequential ones. You need  mpirun to run these scripts. Please consult the usage
clause of these scripts for more detail.

### Sparse Array
For sparse arrays, we provide similar scripts as dense arrays with similar
naming conventions. For example, sparse_create, sparse_read and sparse_load
scripts will create, read and load a given sparse array respectively. However,
the provided scripts are used on AIS vessel traffic data. Please refer to the
'datagen/README' for further notes on AIS data.

### License
MIT 2016
