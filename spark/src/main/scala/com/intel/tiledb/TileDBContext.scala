/**
 * [[com.intel.tiledb.TileDBContext]]
 *
 *
 * The MIT License
 *
 * Copyright (c) 2015 Intel Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package com.intel.tiledb

import java.io.IOException

import com.intel.tiledb.codes.{PartitionScheme, TileDBIOMode}
import com.intel.tiledb.datatypes.TileDBAttribute
import com.intel.tiledb.ordering.{CellOrder, TileOrder}
import org.apache.spark.sql.types.DataType
import org.apache.spark.{Logging, SparkContext}

import scala.io.Source
import scala.reflect.ClassTag
import scala.util.Sorting

class TileDBContext(@transient val tileDBConf: TileDBConf) extends Logging {

  if (tileDBConf.getWorkspaces.length == 0) {
    throw new IllegalArgumentException("TILEDB workspace not set in TileDBConf")
  } else {
    for (i <- tileDBConf.getWorkspaces) {
      if (i.isEmpty) {
        throw new IllegalArgumentException("TILEDB workspace is null")
      }
    }
  }

  /** Initialize Spark context:
   * 1. Set the delay to infinity to enable scheduler to find the right location
   * 2. Load preferred locations in openArray from TileDB storage manager
   */
  @transient val sparkContext =
    new SparkContext(tileDBConf.sparkConf.set("spark.locality.wait", "10000s"))

  // log out Spark Version in Spark driver log
  logInfo(s"Running TileDB-Spark version $TILEDB_SPARK_VERSION")
  logInfo("TileDB Workspaces: " + tileDBConf.getWorkspaces.mkString(","))

  /**
   * Check if the array has already been opened
   *
   * @param arrayName  name of the array
   * @return           true if array is in open schemas, false otherwise
   */
  def isOpen(arrayName: String): Boolean = {
    openSchemas.get(arrayName) != null
  }

  val startTime = System.currentTimeMillis()

  /** Map between array names and their schema objects */
  @transient val openSchemas = collection.mutable.HashMap[String, (Int, TileDBSchema)]()

  /** Map of array name and its partitions. For example,
    * ARRAYNAME -> ((node1 -> (0.0,100.0), (node2 -> (101.0, 200.0))
    */
  @transient val partitionMap =
    collection.mutable.HashMap[String, Map[String, Array[Double]]]()

  @transient val partitionOrderMap =
    collection.mutable.HashMap[String, PartitionScheme.Value]()

  /** Creates thread local copies of local variables */
  @transient
  protected[tiledb] val tlSession = new ThreadLocal[TileDBSession]() {
    override def initialValue: TileDBSession = defaultSession
  }

  @transient
  protected[tiledb] val defaultSession = createSession(tileDBConf)

  /* Thread-safe copy of TileDB configuration */
//  protected[tiledb] def conf = currentSession().conf
  protected[tiledb] def conf = tileDBConf

  val tileDBFactory = new TileDBFactory(conf.getWorkspaces).initContexts(conf.getNumInstances)

  def getArrayId(name: String) : Int = {
    assert(isOpen(name))
    openSchemas(name)._1
  }

  /** Set value of a configuration parameter */
  def setConf(key: String, value: String): TileDBConf = {
    conf.set(key, value)
  }

  /**
   * Return the schema of a given array
   *
   * @param arrayName  Name of the array
   * @return
   */
  def getSchema(arrayName: String) : TileDBSchema = {
    openSchemas(arrayName)._2
  }

  /**
   * Set the preferred locations of array partitions
   *
   * @param arrayName        Name of the array
   * @param partitionScheme  Partitioning scheme of the array between different nodes
   * @param partitionMap     Optional: Mapping of nodes and array partitions
   * @return
   */
  def setPreferredLocations(
    arrayName: String,
    partitionScheme: PartitionScheme.Value = PartitionScheme.ROW_MAJOR,
    partitionMap: Map[String, Array[Double]] = null): TileDBContext = {

    assert(!openSchemas.contains(arrayName))
    this.partitionMap(arrayName) = partitionMap
    partitionOrderMap(arrayName) = partitionScheme
    this
  }

  /**
   * The information of array partitions is stored in a file
   * in native TileDB storage manager. The structure of the
   * file is as follows:
   * 1. ROW_MAJOR partition:
   *      ---------------------------------------------------------------
   *      | node-name, row-lo, row-hi, row-lo, row-hi, row-lo, row-hi ...
   *      ---------------------------------------------------------------
   * 2. COL_MAJOR partition:
   *      ---------------------------------------------------------------
   *      | node-name, col-lo, col-hi, col-lo, col-hi, col-lo, col-hi ...
   *      ---------------------------------------------------------------
   * 3. HILBERT partition:
   *      -------------------------------------------------
   *      | node-name, row-lo, col-lo, row-hi, col-hi, ...
   *      -------------------------------------------------
   *
   * @param arrayName  name of the array
   */
  def readPreferredLocations(arrayName: String):
    (PartitionScheme.Value, Map[String, Array[Double]]) = {

    val partitionMap = new collection.mutable.HashMap[String, Array[Double]]()
    val filename = tileDBConf.getWorkspaces(0) + "/StorageManager/" + "__" +
      arrayName + ".partitions"
    var partitionOrder = PartitionScheme.ROW_MAJOR
    var name = ""

    for ((line, index) <- Source.fromFile(filename).getLines().zipWithIndex) {

      // Header contains the array name and partition scheme
      if (index == 0) {
        val words = line.split(',')
        name = words(0)
        partitionOrder = PartitionScheme(words(1))

      } else {

      // Rest of the file has the structure as described above
        val words = line.split(':')
        val nodename: String = words(0)
//        words.drop(1)
        for (w <- words.drop(1)) {
          val dims = w.split(',')
          partitionOrder match {
            case (PartitionScheme.ROW_MAJOR | PartitionScheme.COL_MAJOR) =>
              val orderedPartitions = dims.map(_.toDouble)
              Sorting.quickSort(orderedPartitions)
              partitionMap += (nodename -> orderedPartitions)
            case PartitionScheme.COL_MAJOR =>
              partitionMap += (words(0) -> words.map(x => x.toDouble).drop(1))
            case x =>
              throw new IllegalArgumentException("Invalid array partition" +
                "order : " + x + " found in partition map file")
          }
        }
      }
    }
    (partitionOrder, partitionMap.toMap)
  }

  def getPartitionSchema(arrayName: String) = partitionOrderMap(arrayName)

//  def getRangeRowLo(arrayName: String, location: String) = {
//    partitionMap(arrayName)(location)._1
//  }
//
//  def getRangeRowHi(arrayName: String, location: String) = {
//    partitionOrderMap(arrayName) match {
//      case PartitionScheme.ROW_MAJOR =>
//        partitionMap(arrayName)(location)._2
//      case x =>
//        partitionMap(arrayName)(location)._3
//    }
//  }
//
//  def getRangeColLo(arrayName: String, location: String) = {
//    partitionOrderMap(arrayName) match {
//      case PartitionScheme.COL_MAJOR =>
//        partitionMap(arrayName)(location)._1
//      case x =>
//        partitionMap(arrayName)(location)._3
//    }
//  }
//
//  def getRangeColHi(arrayName: String, location: String) = {
//    partitionOrderMap(arrayName) match {
//      case PartitionScheme.COL_MAJOR =>
//        partitionMap(arrayName)(location)._2
//      case x =>
//        partitionMap(arrayName)(location)._4
//    }
//  }

  /**
   * The array schema is defined and serialized in TileDB. If the TileDBContext is lost
   * for some reason, use the [[openArray()]] to reload it
   *
   * @param arrayName      Name of the array
   * @param dimensionType  Data type of the array dimensions
   * @param dimensions     Map containing names and data types of the array dimensions
   * @return
   */
  def defineArray(arrayName: String, dimensionType: String,
                  dimensions: Array[(String, (Double, Double))],
                  attributes: Array[(String, String, Boolean, Int)],
                  hasIrregularTiles: Boolean,
                  cellOrder : CellOrder.Value = CellOrder.COL_MAJOR,
                  cellSize: Int = 6,
                  tileOrder : TileOrder.Value = TileOrder.COL_MAJOR,
                  partitionScheme: PartitionScheme.Value = PartitionScheme.ROW_MAJOR,
                  partitionMap: Map[String, Array[Double]] = null) : TileDBSchema = {

    if (openSchemas.contains(arrayName) ||
      tileDBFactory.isDefined(arrayName))
      throw new IllegalArgumentException("ERROR: Array " + arrayName + " already defined")

    val attrObjects = attributes.map(x => {
      new TileDBAttribute(x._1, x._2, x._3, x._4)
    })
    val schema = new TileDBSchema(arrayName, dimensionType, dimensions,
      attrObjects, hasIrregularTiles, cellOrder, cellSize, tileOrder)

    tileDBFactory.defineArray(arrayName, schema.toString())

    /* Serialize the preferred locations of the array partitions */
    setPreferredLocations(arrayName, partitionScheme, partitionMap)

    val id = tileDBFactory.openArray(arrayName, TileDBIOMode.READ)
    tileDBFactory.closeArray(arrayName)
    openSchemas += (arrayName -> (id, schema))
    schema
  }

  /**
   * Load the array schema from native TileDB storage manager
   *
   * @param arrayName  name of the array
   * @param arrayId    descriptor of the array
   * @return
   */
  def loadSchema(arrayName: String, arrayId: Int) : TileDBSchema = {
    tileDBFactory.getArraySchema(arrayName)
  }

  /** Open an array in TileDB
    *
    * @param arrayName  Name of the array
    * @param mode       Mode can be read or write
    * @return
    */
  def openArray(arrayName : String, mode: String) : TileDBArray[DataType] = {
    assert(!openSchemas.contains(arrayName))
    val f = mode.equals(TileDBIOMode.READ) |
      mode.equals(TileDBIOMode.WRITE) |
      mode.equals(TileDBIOMode.APPEND)

    if (!f) {
      throw new IllegalArgumentException("Invalid mode: " + mode)
    }

    val arrayId = tileDBFactory.openArray(arrayName, mode)

    val schema = loadSchema(arrayName, arrayId)
    if (schema == null) {
      throw new IOException("ERROR: Schema for array " +
        arrayName + " not found")
    }

    openSchemas += (arrayName -> (arrayId, schema))
    val partitionTuple2 = readPreferredLocations(arrayName)
    partitionOrderMap(arrayName) = partitionTuple2._1
    partitionMap(arrayName) = partitionTuple2._2
    logInfo("Preferred locations file read successfully for array " + arrayName)
    logInfo("Array " + arrayName + " opened successfully")
    val numTileDBInstances = partitionMap(arrayName).keySet.size

    new TileDBArray(numTileDBInstances, this, arrayName)
  }

  /**
   * Close the opened array
   *
   * @param arrayName  Name of the array
   * @return           Error code
   */
  def closeArray(arrayName: String): Integer = {
    openSchemas.remove(arrayName)
    tileDBFactory.close_array(arrayName)
  }

  /** Persist the array back to native TileDB
    *
    *
    * @param array  TileDB array object in Scala
    * @return
    */
  def persistArray[D: ClassTag](array: TileDBArray[D]) = {
    // TODO: complete storage task
//    tileDBFactory.storeCells[D](array.name, TileDBIOMode.WRITE, 0, )
  }

  /**
   * Get the id for a given array
   *
   * @param arrayName  name of the array
   * @return           id of the array
   */
  def getId(arrayName: String): Int = openSchemas.get(arrayName).get._1

  /**
   * TileDB Session methods
   */
  protected[tiledb] def openSession(tc: TileDBConf): TileDBSession = {
    detachSession()
    val session = createSession(tc)
    tlSession.set(session)

    session
  }

  protected[tiledb] def currentSession(): TileDBSession = {
    if (tlSession == null)
      throw new IllegalArgumentException("tlSession is null")
    tlSession.get()
  }

  protected[tiledb] def createSession(tc: TileDBConf): TileDBSession = {
    new this.TileDBSession(tc)
  }

  protected[tiledb] def setSession(session: TileDBSession): Unit = {
    detachSession()
    tlSession.set(session)
  }

  protected[tiledb] class TileDBSession(tConf: TileDBConf) {
    // Note that this is a lazy val so we can override the default value in subclasses.
    protected[tiledb] lazy val conf: TileDBConf = tConf.clone()
  }

  protected[tiledb] def detachSession(): Unit = {
    tlSession.remove()
  }
}

