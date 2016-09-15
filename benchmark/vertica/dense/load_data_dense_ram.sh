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

# Description: This script loads dense array from a Tab-separated file
#              to Vertica RAM array model. The input array is organized
#							 in contigous tiles where both the tile order and
#							 cells in tiles are in ROW-MAJOR. Edit the filename
#              before running this script. The tablenames are hard-coded
#              for now. Please consult the respective Vertica SQL files
#              for further details.

file=/mnt/hdd2/kushald/tiledb-data/dense_50000x20000_2500x1000_ram.tsv
tablename=dense_50000x20000_2500x1000_ram_withrle
vsql -f ./delete_dense_table_ram.sql
vsql -f ./define_dense_schema_ram.sql
vsql -v input_file="'$file'" -v tablename="$tablename" -f ./load_data_ram.sql

time sync
