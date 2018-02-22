package com.intel.tiledb.launcher

import com.intel.tiledb.codes.TileDBIOMode
import com.intel.tiledb.{TileDBConf, TileDBContext}
import org.apache.spark.SparkConf

/**
 * Query DB Parse Logic:
 * 1. Get mapping of sample ids to array id (partitions id corresponding to every node)
 * $ query_db.py -d sqlite:////home/sparkuser/VariantDB/meta_data/samples_and_fields.sqlite
 *     -j '{ "query":"ArrayIdx2SampleIdx","input":{"array_idx":2},"output":"/tmp/t.csv"}'
 */

object GenomeLauncher {

  val USAGE = "Usage: spark-submit --class com.intel.tiledb.GenomeLauncher" +
                " array-name spark-master\n" +
                "\tThese command options must be provided in this order\n"

  def main(args: Array[String]) = {

    val tuple3 = parseArgs(args)
    val arrayName: String = tuple3._1
    val master: String = tuple3._2


    /* Initialize a Spark configuration */
    val conf = new SparkConf()
      .setAppName("TileDB-Spark")
      .setMaster(master)

    /* Initialize a TileDB configuration */
    val tc = new TileDBConf(true, conf)
    tc.setNumInstances(1)

    /* Initialize TileDB context */
    val tCtxt = new TileDBContext(tc)

    val myArray = tCtxt.openArray(arrayName, TileDBIOMode.READ)

    val ne = myArray.count()

    System.out.println("********************************************************")
    System.out.println("Number of elements in " + arrayName + " = " + ne)
    System.out.println("********************************************************")

    myArray.collect().foreach(println)
  }

  def parseArgs(args: Array[String]): (String, String, String) = {

    if (args.length < 3) {
      System.out.println(USAGE)
      System.exit(1)
    }

    val arrayName = args(0)
    val sparkMaster = args(1)
    val arrayPartitionFile = args(2)
    (arrayName, sparkMaster, arrayPartitionFile)
  }
}
