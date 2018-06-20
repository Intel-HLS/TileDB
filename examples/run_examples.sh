#!/bin/bash

#The MIT License (MIT)
#Copyright (c) 2018 Omics Data Automation Inc. and Intel Corporation

#Permission is hereby granted, free of charge, to any person obtaining a copy of
#this software and associated documentation files (the "Software"), to deal in
#the Software without restriction, including without limitation the rights to
#use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
#the Software, and to permit persons to whom the Software is furnished to do so,
#subject to the following conditions:

#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
#FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
#COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
#IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
#CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


#This runs a small subset of example binariesi from <build>/examples/ directory. 

#Usage without arguments(workspace will be a default "my_workspace" in <build/examples/) :
#   ./run_examples.sh 
#Usage with args(path can either be a posix filesystem path or a hdfs/emrfs/gs URL) 
#   ./run_examples.sh <path>
#Results are either in a "log" file for the default case or <last_segment_of_specified_path>.log
#if path is specified.
# e.g for ./run_examples.sh gs://my_bucket/my_dir/my_test, expect results in test.log
#Check the log file against the <install>/examples/expected_results file. 

run_example() {
  if [[ $# -eq 3 ]]
  then
    logfile=`basename $2`.log
    echo "Example $3: Running $1..." | tee -a ${logfile}
    $1 $2 | tee -a ${logfile}
        echo "Example $3: Done running $1" | tee -a ${logfile}
  else
    echo "Example $2: Running $1..." | tee -a log
    $1 | tee -a log
        echo "Example $2: Done running $1" | tee -a log
  fi
}

if [[ -n $1 ]]
then
  logfile=`basename $1`.log
  if [[ $1 == *"//"* ]]
  then
    echo "Cleaning hdfs $1" | tee ${logfile}
    hdfs dfs -rm -r $1 | tee -a ${logfile}
  else
    echo "Cleaning posixfs$1" | tee ${logfile}
    rm -fr $1 my_workspace | tee -a ${logfile}
  fi
else
  echo "Cleaning default my_workspace" | tee log
  rm -fr my_workspace | tee -a log
fi

run_example ./tiledb_workspace_group_create $1 1
run_example ./tiledb_array_create_dense $1 2
run_example ./tiledb_array_create_sparse $1 3
run_example ./tiledb_array_primitive $1 4
run_example ./tiledb_array_write_dense_1 $1 5
run_example ./tiledb_array_write_sparse_1 $1 6
run_example ./tiledb_array_read_dense_1 $1 7
run_example ./tiledb_array_write_dense_2 $1 8
run_example ./tiledb_array_read_dense_1 $1 9
run_example ./tiledb_array_read_dense_2 $1 10
run_example ./tiledb_array_write_dense_sorted $1 11
run_example ./tiledb_array_read_dense_1 $1 12
run_example ./tiledb_array_update_dense_1 $1 13
run_example ./tiledb_array_read_dense_1 $1 14
run_example ./tiledb_array_update_dense_2 $1 15
run_example ./tiledb_array_read_dense_1 $1 16
run_example ./tiledb_array_write_sparse_1 $1 17
run_example ./tiledb_array_read_sparse_1 $1 18
run_example ./tiledb_array_read_sparse_2 $1 19
run_example ./tiledb_array_write_sparse_2 $1 20
run_example ./tiledb_array_read_sparse_1 $1 21
run_example ./tiledb_array_read_sparse_2 $1 22
run_example ./tiledb_array_update_sparse_1 $1 23
run_example ./tiledb_array_read_sparse_1 $1 24
run_example ./tiledb_array_read_sparse_2 $1 25
run_example ./tiledb_array_consolidate $1 26
run_example ./tiledb_array_read_dense_1 $1 27
run_example ./tiledb_array_read_sparse_1 $1 28
run_example ./tiledb_array_read_dense_2 $1 29
run_example ./tiledb_array_read_sparse_2 $1 30

