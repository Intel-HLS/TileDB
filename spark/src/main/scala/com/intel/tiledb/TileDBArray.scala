/**
 * [[com.intel.tiledb.TileDBArray]]
 * @author Kushal Datta <kushal.datta@intel.com>
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
 *
 * ==Description==
 * Manages the TileDB array objects in Spark
 */

package com.intel.tiledb

import com.intel.tiledb.codes.PartitionScheme
import com.intel.tiledb.datatypes.TileDBDataType
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Logging, Partition, Partitioner, TaskContext}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * TileDB array is automatically initialized with number of the
 * nodes in the cluster. Right now, the assumption is that there
 * is one to one mapping between RDD partitions with TileDB
 * instances (or nodes)
 *
 * @param parallelism    Number of TileDB nodes in the cluster
 * @param tileDBContext  TileDB context stored in driver memory
 * @param arrayName      Name of the array (same array fragments
 *                       must be present in all the instances of
 *                       the underlying native TileDB storage
 * @tparam D             Dimension type
 */
@Experimental
class TileDBArray[D: ClassTag]
    (parallelism: Int, @transient tileDBContext: TileDBContext, arrayName: String)
  extends RDD[TileDBCell[D]](tileDBContext.sparkContext, Nil) with Logging {

  val workspaces = tileDBContext.conf.getWorkspaces
  val tileDBArrayId = tileDBContext.getArrayId(arrayName)
  val schema = tileDBContext.getSchema(arrayName)
  val dimType = if (schema != null) schema.getDimensionType else TileDBDataType.INVALID
  val segmentSize = tileDBContext.conf.getConf("spark.tiledb.segment.size",
    TileDBArray.TILEDB_SEGMENT_SIZE_DEFAULT).toInt

  val bcast_arrayPartitions =
    tileDBContext.sparkContext.broadcast(tileDBContext.partitionMap(arrayName))
  val bcast_arrayPartitionSchema =
    tileDBContext.sparkContext.broadcast(tileDBContext.partitionOrderMap(arrayName))

  val importAttributeList = new ArrayBuffer[String]()

  /**
   * RDD compute function called by DAGScheduler
   *
   * @param thisSplit  The current TileDB array RDD partition
   * @param context    Task context of Spark RDD
   * @return           Iterator for TileDB cells. It does multiple JNI calls.
   *                   Please refer to [[com.intel.tiledb.TileDBCellPartition]]
   */
  override def compute(thisSplit: Partition, context: TaskContext): Iterator[TileDBCell[D]] = {
    val split = thisSplit.asInstanceOf[TileDBCellPartition[D]]
    split.iterator()
  }

  // TODO: Need to do something here
  override val partitioner: Option[Partitioner] = None

  /**
   * Load TileDB cells into RDDs. Each partition corresponds
   * to an array partition in a node. The mapping currently
   * is assumed to be specified in
   * $TILEDB_WORKSPACE/StorageManager/&#95;&#95;ARRAYNAME.partitions
   * file
   *
   * @return
   */
  override protected def getPartitions: Array[Partition] = {

    assert(tileDBContext.isOpen(arrayName))

    /* Number of partitions must correspond to number of nodes */
    val partitions = parallelism

    val partitionList = tileDBContext.partitionMap(arrayName).toArray
    val array = new Array[Partition](partitions)
    for (i <- 0 until partitions) {
      dimType match {
        case TileDBDataType.CHAR =>
          array(i) = new TileDBCellPartition[Char](workspaces,
            schema, segmentSize, i, id, arrayName, tileDBArrayId,
            partitionList(i)._2, importAttributeList.toArray)
        case TileDBDataType.INT =>
          array(i) = new TileDBCellPartition[Int](workspaces,
            schema, segmentSize, i, id, arrayName, tileDBArrayId,
            partitionList(i)._2, importAttributeList.toArray)
        case TileDBDataType.INT64 =>
          array(i) = new TileDBCellPartition[Long](workspaces,
            schema, segmentSize, i, id, arrayName, tileDBArrayId,
            partitionList(i)._2, importAttributeList.toArray)
        case TileDBDataType.FLOAT =>
          array(i) = new TileDBCellPartition[Float](workspaces,
            schema, segmentSize, i, id, arrayName, tileDBArrayId,
            partitionList(i)._2, importAttributeList.toArray)
        case TileDBDataType.DOUBLE =>
          array(i) = new TileDBCellPartition[Double](workspaces,
            schema, segmentSize, i, id, arrayName, tileDBArrayId,
            partitionList(i)._2, importAttributeList.toArray)
        case TileDBDataType.INVALID =>
          throw new IllegalArgumentException("Null dimension type found for array: " + arrayName)
      }
    }
    array
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    logDebug("Finding preferred location for " + this + ", partition " + split.index)
    val thisSplit = split.asInstanceOf[TileDBCellPartition[D]]
    val locations = ArrayBuffer[String]()
    bcast_arrayPartitionSchema.value match {
      case PartitionScheme.ROW_MAJOR =>
        val partitionArray = bcast_arrayPartitions.value.toArray
        for (node2rowTuple <- partitionArray) {
          /**
           * The assumption here is that TileDB array partitions
           * are never replicated. That is why only checking the
           * location of any (first in this case) element will
           * give us the array location. If this assumption breaks,
           * the code will break
           */
          if (thisSplit.partitionList.contains(node2rowTuple._2(0))) {
            locations += node2rowTuple._1
          }
        }
      case _ =>
        // TODO: Handle other cases
        throw new IllegalArgumentException("Partition scheme is not row major;" +
          " Check the array partitions file")
    }
    if (locations.isEmpty)
      throw new IllegalArgumentException("Preferred location not found." +
        "Check the array partitions file")

    locations.toSeq
  }

  def setImportAttributeList(attrList: Array[String]) = {
    for (attrName <- attrList)
      importAttributeList += attrName
  }

  /**
   * Cut a block of the array
   *
   * @param ranges  Range of the dimensions to cut the block out of array
   * @return        Returns another sparse array
   */
  // TODO: implement subArray
  def cut(ranges: Map[String, Range]) : this.type = {
    this
  }

  override def count(): Long = {
    super.count()
  }

  override def take(num: Int): Array[TileDBCell[D]] = {
    super.take(num)
  }

  override def collect(): Array[TileDBCell[D]] = {
    super.collect()
  }

  // TODO: Convert sparse array elements to DF rows
  @scala.annotation.varargs
  def toDF(colNames: String*) : DataFrame = {
//    val newCols = logicalPlan.output.zip(colNames).map { case (oldAttribute, newName) =>
//      Column(oldAttribute).as(newName)
//    }
//    select(newCols : _*)
    null
  }

  def mapPartitions(f: Partition => Unit) = {

  }

  /** Default persist behavior is to cache it in memory and serialize to native TileDB
    *
    * @return  the RDD
    */
  override def persist(): this.type = {
    persist(StorageLevel.MEMORY_AND_DISK)
  }

  /** Persist to a given storage level. The behavior matches
    * the default Spark RDD behavior
    *
    * @param storageLevel  Memory, disk in serialized or unserialized fashion
    * @return              the RDD
    */

  override def persist(storageLevel: StorageLevel): this.type = {
    if (storageLevel.equals(StorageLevel.MEMORY_ONLY)) {
      super.persist(storageLevel)
    } else {
      tileDBContext.persistArray(this)
    }
    this
  }

  /**
    * Label or feature names must come from TileDBSchema. The label can be any of the attributes.
    * Features can be either dimensions or attributes
    *
    * @param label
    * @param features
    * @return
    */
  def toLabeledPointRDD(label: String, features: Array[String]): RDD[LabeledPoint] = {
    if (!schema.getAttributeNames.contains(label))
      throw new IllegalArgumentException("Invalid label name passed to TileDB array")

    this.map(_.toLabeledPoint(label, features))
  }
}

object TileDBArray {
  val TILEDB_SEGMENT_SIZE_DEFAULT = "2000000"
}
