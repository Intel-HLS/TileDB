#!/bin/bash

if [ -z "$SPARK_HOME" ]; then
  echo "SPARK_HOME not set"
  exit
fi

CLASS=com.intel.tiledb.launcher.GenomeLauncher
TILEDB_JAR=tiledb-spark-1.0-SNAPSHOT.jar
TILEDB_SPARK_HOME="$(cd `dirname $0`; pwd)"

. $TILEDB_SPARK_HOME/../conf/tiledb-env.sh

if [ -z "$TILEDB_HOME" ]; then
  echo "TILEDB_HOME not set"
  exit
fi

$SPARK_HOME/bin/spark-submit \
  --deploy-mode client \
  --class $CLASS \
  --driver-class-path $TILEDB_SPARK_HOME/../target/$TILEDB_JAR \
  --driver-library-path $TILEDB_SPARK_HOME/../lib:/opt/gcc-5.1.0/lib64:$LD_LIBRARY_PATH \
  --conf spark.executor.extraLibraryPath=$TILEDB_SPARK_HOME/lib:/opt/gcc-5.1.0/lib64:$LD_LIBRARY_PATH \
  --conf spark.executor.extraClassPath=$TILEDB_SPARK_HOME/../target/$TILEDB_JAR \
  --conf spark.tiledb.workspace.dir=$TILEDB_WORKSPACE \
  target/$TILEDB_JAR \
  "$@"
