# Vertica(R) Benchmarking Scripts
This folder contains the scripts to benchmark load, read, update, random read,
parallel load, parallel read parallel update, and consolidation operations in
Vertica.

### Dense Array
Dense arrays can be modeled in multiple ways in a relational database. The
easiest way is to have the coordinates as primary keys. However, this is not
optimal when the reads occur as chunks. To enable faster reads for chunks, we
added tile id to the primary key. Hence for a 2D array, the data is ordered as
(X,Y,TILE ID). In many scientific applications arrays are accessed using the
global address space. We benchmark such a data model, the RAM model to be
precise where the primary key is the counter of cells in the global order (i.e.
following row-major or col-major order as defined by user). The rest of the
schema in this model contain the attributes.

Generate the dense arrays with the script
'datagen/generate_dense_array_for_vertica.cc'

To create required schema and load a 2D dense array to Vertica using GZIP
compression, use the following command:
```sh
$ load_data_dense.sh [0|1] -- 0 signifies without tile id, 1 signifies with tile
id
```

To create required schema and load a 2D dense array following the RAM model
using GZIP compression, use
```sh
$ load_data_dense_ram.sh
```

To create and 2D dense array using RLE encoding, use:
```sh
$ load_data_dense_withrle.sh [0|1] -- 0 signifies without tile id, 1 signifies
with tile id
```

To read blocks of the 2D dense array, use the corresponding query-*.sql script
directly from 'vsql' - Vertica's command line utility. For example, to query one
chunk of a 2D array where the tablename is XYZ, use:
```sh
$ vsql -v tablename=XYZ -f ./query-dense-one-chunk.sql
```

To read random locations from an array (or table in Vertica), use:
```sh
$ vsql -v inputfile="'randomreadlocations'" -v tablename=XYZ -f
./random-dense.sql
```
In the above case, the input random read locations file can be generated using
the script 'datagen/generate_random_coordinates_for_random_reads.cc'

### Sparse Array
Similar to dense arrays, sparse arrays are represented in Vertica in two ways -
with or without tile id. Again, sorting the cells according to tile id favors
chunked reads. The naming convenion of sparse array scripts is similar to those
for dense arrays.

To load 2D sparse arrays, use the load scripts as shown here. Be cognizant that
the these scripts are provided for AIS data. So, follow the datagen README to
organize the AIS data as expected by these scripts.
```sh
$ bash load_data_sparse.sh
```

Scripts to read sparse/dense regions in the AIS data are provided. For example,
to read 10,000 elements from a sparse region of the sparse array, use:
```sh
$ vsql -f query_box_10k-sparseregion.sql
```

To read random locations in a given region of the sparse array, use the
random_sparse_query.sh script. Provide the necessary range, for example to read
random locations in a box 100 to 200 (in X direction) and 0 to 40 (in direction
Y), use:
```sh
$ bash random_sparse_query.sh 100 200 0 40
```

### License
MIT 2016

