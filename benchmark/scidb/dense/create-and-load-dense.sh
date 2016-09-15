#!/bin/bash

# The MIT License (MIT)
#
# Copyright (c) 2016 MIT and Intel Corporation
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

#
# This script takes the dimension ranges and chunk dimensions of a dense
# array as input, creates the array in SciDB and loads the binary chunks
# (saved as m_array-qualifier.bin) under the corresponding SciDB instance
# directories.
#
# Prerequisites: The binary files containing data from the chunks must be
#                already present under the SciDB instance directories
#                NOTE: Please check the data generator to create the binary
#                chunks
#

DIM0=$1
DIM1=$2
CHUNKDIM0=$3
CHUNKDIM1=$4
ARRAY_QUALIFIER=${DIM0}x${DIM1}_${CHUNKDIM0}x${CHUNKDIM1}
ARRAY_NAME=dense\_${ARRAY_QUALIFIER}
echo $ARRAY_NAME
echo m${ARRAY_QUALIFIER}.bin
iquery -naq "remove($ARRAY_NAME);"
iquery -naq "create array $ARRAY_NAME <a1:int32>[i=0:$(echo "$DIM0 - 1" | bc),$CHUNKDIM0,0,j=0:$(echo "$DIM1 - 1" | bc),$CHUNKDIM1,0];"
time timeout 3600s iquery -t -naq "store(input($ARRAY_NAME,'m${ARRAY_QUALIFIER}.bin',-1,'(int32)'),$ARRAY_NAME);"
