#!/bin/bash

# The MIT License
#
# Copyright (c) 2016 MIT and Intel Corp.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

# Description: If W=1, then this script reloads a dense array with tile id,
#							 else it reloads without tile id. In the first case, tile id
#              is added as the primary key so that queries local to a chunk
#              can exploit locality. The input array data file contains
#              values as X,Y,TID,Value where X and Y are the dimensions,
#              TID is the tile id

W=$1

if [ "$W" == "1" ]; then
	echo "W=1"
	file=/mnt/hdd2/kushald/tiledb-data/dense_50000x20000_2500x1000_withtileid.tsv
	tablename=dense_50000x20000_2500x1000_withtileid
	vsql -f ./delete_dense_table_withtileid.sql
	vsql -f ./define_dense_schema_withtileid.sql
	vsql -v input_file="'$file'" -v tablename="$tablename" -f ./load_data.sql
else
	echo "W=0"
	file=/mnt/hdd2/kushald/tiledb-data/dense_50000x20000_2500x1000.tsv
	tablename=dense_50000x20000_2500x1000
	vsql -f ./delete_dense_table.sql
	vsql -f ./define_dense_schema.sql
	vsql -v input_file="'$file'" -v tablename="$tablename" -f ./load_data.sql
fi

time sync
