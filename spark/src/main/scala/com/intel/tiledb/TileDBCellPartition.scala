/**
 * [[com.intel.tiledb.TileDBCell]]
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
 *
 */

package com.intel.tiledb

import com.intel.tiledb.codes.{TileDBErrorCode, TileDBIOMode}
import org.apache.spark.{Logging, Partition, TaskContext}

import scala.reflect.ClassTag

/**
 * A TileDB cell has the following structure:
 *   (c0,c1,c2,c3,...,cD,a0,a1,a2,a3,...,aN)
 *   where the array has D dimensions and N attributes
 *
 * The TileDBPartition class implements the vertical representation
 * of arrays in a RDD
 *
 * D is the dimension type which can be of type integer, int64_t, float
 * or double
 *
 * @param workspaces  TileDB workspaces
 *
 */
class TileDBCellPartition
  [@specialized(Int, Long, Float, Double) D: ClassTag](
    val workspaces: Array[String],
    val schema: TileDBSchema,
    val segmentSize: Int,
    val idx: Int,
    val rddId: Int,
    val arrayName: String,
    val arrayId: Int,
    val orderedPartitionList: Array[Double],
    val importAttrList: Array[String])
  extends Partition with Logging {

  override def hashCode(): Int = 41 * (41 + rddId) + idx

  var cells : Array[TileDBCell[D]] = null
  var rddSize: Int = 0
  val bufferedCells = new Array[TileDBCell[D]](segmentSize)
  var isOpen = false
  var nativeCellIteratorInitialized = false
  val partitionList = orderedPartitionList
  var array_local_id = 0
  var mySchema: TileDBSchema = null

  /**
   * Load the cells from native TileDB instances
   *
   * @param context  Task context as set by Spark Context
   */
  def loadCells(tf: TileDBFactory, context: TaskContext): Unit = {
    cells = new Array[TileDBCell[D]](segmentSize)
    array_local_id = tf.openArray(arrayName, TileDBIOMode.READ)
    val schema = tf.getArraySchema(arrayName)
    val attributeList = schema.getAttributeNames
    val dimList = schema.getDimensionNames
    rddSize = tf.loadCells[D](arrayName, schema, dimList, attributeList, rddId, cells)
//    logInfo("Loaded " + rddSize + " cells in array " + arrayName)
  }

  def loadNextCells(tf: TileDBFactory): Int = {

    if (!isOpen) {
      isOpen = true
      array_local_id = tf.openArray(arrayName, TileDBIOMode.READ)
      mySchema = tf.getArraySchema(arrayName)
    }
    val attributeList = {
      if (importAttrList.isEmpty)
        mySchema.getAttributeNames
      else
        importAttrList
    }
    val dimList = mySchema.getDimensionNames
    if (!nativeCellIteratorInitialized) {
      val ret = tf.initialize_const_cell_iterator(arrayId, attributeList)
      TileDBErrorCode(ret)
      nativeCellIteratorInitialized = true
    }
    val errorcode = tf.loadNCells(arrayName, mySchema, dimList, attributeList, rddId,
      segmentSize, bufferedCells)
//    val errorcode = tf.loadCells(arrayName, mySchema, dimList, attributeList, rddId,
//        bufferedCells)
    if (errorcode < TileDBErrorCode.TILEDB_OK)
      TileDBErrorCode(errorcode)
    /* Return the number of cells retrieved */
    errorcode
  }

  /**
   * Cell iterator
   *
   * @return
   */
  def iterator(): Iterator[TileDBCell[D]] = new Iterator[TileDBCell[D]] {
    private[this] var pos = 0
    private[this] var valid = false
    private[this] var numCellsLoaded = 0

    val tf: TileDBFactory = new TileDBFactory(workspaces).initContexts(1)

    override def hasNext: Boolean = {
      if (!valid) {
        numCellsLoaded = loadNextCells(tf)
        valid = numCellsLoaded > 0
        pos = 0
      } else {
        valid = pos < numCellsLoaded
      }
      valid
    }

    override def next(): TileDBCell[D] = {
      assert(valid)
      val cell = bufferedCells(pos)
      pos += 1
      valid = pos < bufferedCells.length
      cell
    }
  }

  /**
   * The dimensions ranges are given in the order of the dimensions.
   *
   * @param ranges  Range of cut in all dimensions
   * @return
   */
  def subArray(ranges: Array[(Any, Any)]) : this.type = {
     null
  }

  override def index: Int = idx

  /**
   * Store this partition back to native TileDB
   * Each RDD partition writes the cells back to local TileDB instance
   */
  def store(): Unit = {
    assert(!workspaces.isEmpty)
    store("w")
  }

  def store(mode: String): Unit = {
    val tf = new TileDBFactory(workspaces)
    tf.storeCells(arrayName, mode, rddId, cells)
  }
}
