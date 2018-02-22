package com.intel.tiledb.examples

import com.intel.tiledb.codes.TileDBIOMode
import com.intel.tiledb.{TileDBConf, TileDBContext}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.regression.{LinearRegressionModel, LinearRegressionWithSGD}

/**
  * Created by kdatta1 on 1/7/16.
  */
object LinearRegression {

  val SPARK_MASTER_DEFAULT = "spark://localhost:7077"

  // Load and parse the data
//  val data = sc.textFile("data/mllib/ridge-data/lpsa.data")
//  val parsedData = data.map { line =>
//    val parts = line.split(',')
//    LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
//  }.cache()
//
//  // Building the model
//  val numIterations = 100
//  val model = LinearRegressionWithSGD.train(parsedData, numIterations)

  def main(args: Array[String]) = {

    val tuple3 = parseArgs(args)
    val arrayName: String = tuple3._1
    val master: String = tuple3._2
    val partitionSize: Int = tuple3._3

    /* Initialize a Spark configuration */
    val conf = new SparkConf()
      .setAppName("MLLib Linera Regression with TileDB Array")
      .setMaster(master)

    /* Initialize a TileDB configuration */
    val tc = new TileDBConf(true, conf)
      //.setWorkspace("/home/kdatta1/TileDB/tiledb-ws/")
      .setPartitionSize(partitionSize)
    tc.setNumInstances(1)

    /* Initialize TileDB context */
    val tCtxt = new TileDBContext(tc)

    val myArray = tCtxt.openArray(arrayName, TileDBIOMode.READ)
    System.out.println("count: " + myArray.count())
    for (i <- myArray.collect()) {
      System.out.println(i)
    }

    val parsedData = myArray.toLabeledPointRDD("attr1",
         Array("dim0", "dim1", "dim2", "dim3", "dim4", "dim5", "dim6", "dim7")).cache()

    // Building the model
    val numIterations = 10
    val model = LinearRegressionWithSGD.train(parsedData, numIterations)

    // Evaluate model on training examples and compute training error
    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
    println("training Mean Squared Error = " + MSE)

    // Save and load model
    model.save(tCtxt.sparkContext, "myModelPath")
    val sameModel = LinearRegressionModel.load(tCtxt.sparkContext, "myModelPath")
  }

  ///////////////////////////////////////////
  /// Other methods
  ///////////////////////////////////////////

  def parseArgs(args: Array[String]) : (String, String, Int) = {
    var arrayName: String = ""
    var partitionSize: Int = 0
    var sparkMaster = ""
    if (args.length < 1) {
      println("Usage: spark-submit --class com.intel.tiledb.examples.LinearRegression" +
        " array-name [spark-master] [partition-size]")
      System.exit(0)
    } else {
      if (args.length == 2) {
        arrayName = args(0)
        sparkMaster = args(1)
        partitionSize = 1000
      } else if (args.length == 1) {
        arrayName = args(0)
        sparkMaster = SPARK_MASTER_DEFAULT
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
