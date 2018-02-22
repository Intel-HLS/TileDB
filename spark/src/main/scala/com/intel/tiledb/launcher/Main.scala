package com.intel.tiledb.launcher

import com.intel.tiledb.codes.TileDBIOMode
import com.intel.tiledb.{TileDBConf, TileDBContext}
import org.apache.spark.SparkConf

object Main {

  def main(args: Array[String]) = {

    val tuple3 = parseArgs(args)
    val arrayName: String = tuple3._1
    val master: String = tuple3._2
    val partitionSize: Int = tuple3._3


    /* Initialize a Spark configuration */
    val conf = new SparkConf()
      .setAppName("TileDB-Spark")
      .setMaster(master)
      //.setSparkHome("/home/kdatta1/Spark/spark")

    /* Initialize a TileDB configuration */
    val tc = new TileDBConf(true, conf)
      //.setWorkspace("/home/kdatta1/TileDB/tiledb-ws/")
      .setPartitionSize(partitionSize)
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


  ///////////////////////////////////////////
  /// Other methods
  ///////////////////////////////////////////

  def parseArgs(args: Array[String]) : (String, String, Int) = {
    var arrayName: String = ""
    var partitionSize: Int = 0
    var sparkMaster = ""
    if (args.length < 2) {
      println("Usage: spark-submit --class com.intel.tiledb.launcher.Main" +
        " array-name spark-master partition-size")
      System.exit(0)
    } else {
      if (args.length == 2) {
        arrayName = args(0)
        sparkMaster = args(1)
        partitionSize = 1000
      } else {
        arrayName = args(0)
        sparkMaster = args(1)
        partitionSize = args(2).toInt
      }
    }
    (arrayName, sparkMaster, partitionSize)
  }

}
