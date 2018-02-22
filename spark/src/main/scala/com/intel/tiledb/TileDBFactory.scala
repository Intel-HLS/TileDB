/**
 * [[com.intel.tiledb.TileDBFactory]]
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
 * TileDBFactory contains the declarations of the native functions
 * required to access TileDB arrays/cells/cell iterators in Scala.
 * The methods are defined in [[com/intel/tiledb/cpp/src/TileDBFactory.cc]]
 */

package com.intel.tiledb

import java.io._

import com.intel.tiledb.codes.TileDBErrorCode
import org.apache.spark.Logging

import scala.io.Source
import scala.reflect.ClassTag

class TileDBFactory(val workspaces: Array[String])
  extends Serializable with Logging with Closeable {

  /**
   *  This will load libTileDB.so from the library path.
   *  Be careful, hard-coded in package object tiledb
   *  defined in [[com.intel.tiledb/package.scala/tiledb]]
   */
  System.loadLibrary(TILEDB_SPARK_LIBRARY_NAME)

  @native def array_defined(workspace: String,
                            name: String) : Boolean
  @native def open_array(workspace: String,
                         arrayName: String,
                         mode: String) : Int
  @native def close_array(name: String): Int
  @native def define_array(workspace: String,
                           name: String,
                           arraySchemaStr: String) : Int
  @native def get_array_id(name: String)
  @native def subarray(id: Int)
  @native def array_schema(jniContext: Long,
                           workspace: String,
                           name: String) : String
  @native def export_csv(name: String,
                         path: String) : Int
  @native def export_bin(name: String,
                         path: String) : Int
  @native def get_fragment_size(s: String) : Int
  @native def get_next_cell(workspace: String,
                            id: Long,
                            attributes: Array[String],
                            iterator: Int): String
  @native def import_all_cells(arrayName: String, fileName: String,
                               dimensions: Array[String],
                               attributes: Array[String]): Int
  @native def import_cells(numCell: Int, dimensionIds: Array[Int],
                           dimensionType: String, attributeIds: Array[Int],
                           cells: Array[String]): Int
  @native def initialize_context(workspace: String): Long
  @native def finalize_context(context: Long): Int
  @native def initialize_const_cell_iterator(arrayId: Int, attributes: Array[String]): Int
  @native def finalize_const_cell_iterator(): Int

  /** Store the pointers for TileDB contexts. These
   *  are dereferenced in JNI code in TileDBFactory.cc
   */
  val tileDBContexts = new Array[Long](workspaces.length)

  /**
   * Check if array is defined in native TileDB or not
   *
   * @param name  name of the array
   * @return      False if array is not defined
   */
  def isDefined(name: String): Boolean = array_defined(workspaces(0), name)

  /**
   * Define array wrapper for native method
   *
   * @param name            name of the array
   * @param arraySchemaStr  Schema in CSV format
   */
  def defineArray(
    name: String,
    arraySchemaStr: String): TileDBErrorCode = {

    new TileDBErrorCode(define_array(workspaces(0), name, arraySchemaStr))
  }

  /**
   * Open an array with the given name and return its descriptor
   *
   * @param arrayName  name of the array
   * @return
   */
  def openArray(arrayName: String, mode: String): Int = {
    assert(tileDBContexts.length > 0)
    val arrayId = open_array(workspaces(0), arrayName, mode)
    if (arrayId == TileDBErrorCode.TILEDB_EPARSE) {
      throw new ArrayIndexOutOfBoundsException("Cannot open array: " + arrayName)
    }
    logInfo("Array " + arrayName + " opened successfully")
    arrayId
  }

  /**
   * Close a given array
   *
   * @param arrayName  name of the array
   * @return
   */
  def closeArray(arrayName: String) = {
    new TileDBErrorCode(close_array(arrayName))
    logInfo("Array " + arrayName + " closed successfully")
  }

  /**
   * Load the schema of a given array from TileDB. If not found, this
   * method throws an IOException
   *
   * @param name  name of the array
   * @return
   */
  def getArraySchema(name: String): TileDBSchema = {
    val schemaAsCSV = array_schema(tileDBContexts(0), workspaces(0), name)
    if (schemaAsCSV.isEmpty)
      return null

    logInfo("Schema as seen in getArraySchema: " + schemaAsCSV)
    TileDBSchema(name, schemaAsCSV)
  }

  def getFragmentSize(arrayName: String): Int = {
    1000
  }

  /**
   * Initialize a TileDB context and store the reference
   * in a integer. This is de-referenced as a pointer in
   * JNI TileDBFactory.cc
   *
   * @return  TileDB context as an integer
   */
  def initContexts(numInstances: Int): this.type = {

    var i = 0
    while (i < numInstances) {
      val tctx = initialize_context(workspaces(i))
      if (!TileDBErrorCode.isError(tctx.toInt)) {
        tileDBContexts(i) = tctx
        i += 1
      } else {
        new TileDBErrorCode(tctx.toInt)
      }
    }
    this
  }

  /**
   * Load the cells from a TileDB fragment
   *
   * @param arrayName   name of the array
   * @param attributes  Names of array attributes
   * @param rddId       RDD partition id
   * @param payload     Buffer of TileDB cells returned
   *
   * @return            Number of cells read from native TileDB
   */
  def loadCells[D: ClassTag](arrayName: String,
                schema: TileDBSchema,
                dimensions: Array[String],
                attributes: Array[String],
                rddId: Int,
                payload: Array[TileDBCell[D]]): Int = {

    val csvFileName = workspaces(0) + "/__tiledb_spark." + arrayName + "." + rddId + ".csv"
    System.out.println("Just before calling import all cells")
    import_all_cells(arrayName, csvFileName, schema.getDimensionNames, attributes)
    logInfo("Successfully exported cells from TileDB")
    var count : Int = 0
    try {
      for (line <- Source.fromFile(csvFileName).getLines()) {
        payload(count) = TileDBCell[D](line, schema)
        count += 1
      }
    } catch {
      case ex: FileNotFoundException => println(csvFileName + ": No such file found")
      case ex: IOException => println("Had an IOException trying to read file " + csvFileName)
    }
    logInfo("Successfully exported " + count + " cells from TileDB")
    count
  }

  /**
   * Load the cells from a TileDB fragment
   *
   * @param arrayName   name of the array
   * @param attributes  Names of array attributes
   * @param rddId       RDD partition id
   * @param payload     Buffer of TileDB cells returned
   *
   * @return            Number of cells read from native TileDB
   */
  def loadNCells[D: ClassTag](arrayName: String,
                             schema: TileDBSchema,
                             dimensions: Array[String],
                             attributes: Array[String],
                             rddId: Int,
                             N: Int = 0,
                             payload: Array[TileDBCell[D]]): Int = {

    assert(dimensions != null)
    assert(attributes != null)

    if (N == 0) {
      return loadCells(arrayName, schema, dimensions, attributes, rddId, payload)
    }

    val cells = new Array[String](N)
    /** If no attributes given, get all */
    val attrList = {
      if (attributes.isEmpty)
        schema.getAttributeIds(null)
      else
        schema.getAttributeIds(attributes)
    }
    val dimList = {
      if (!dimensions.isEmpty)
        schema.getDimensionIds(null)
      else
        schema.getDimensionIds(dimensions)
    }
    val ret = import_cells(N, dimList, schema.getDimensionType, attrList, cells)
//    logDebug("ret = " + ret)
//    for (i <- cells) logDebug(i.toString)
    if (ret > 0) {
      for (i <- 0 until ret) {
//        logInfo("cell " + i.toString + " is: " + cells(i).toString)
        payload(i) = TileDBCell[D](cells(i), schema, dimensions, attributes)
      }
    } else {
      TileDBErrorCode(ret)
    }
    logInfo("TileDB loadcells completed. Loaded " + ret + " cells")
    /* Return the number of cells retrieved */
    ret
  }

  /**
   * Store the cells back to native TileDB
   *
   * @param cells  Cells to be written back to TileDB storage
   * @return       Erro code
   */
  def storeCells[D: ClassTag](
    arrayName: String,
    mode: String,
    rddId: Int,
    cells: Array[TileDBCell[D]]) = {

    assert(!mode.equals("r"))
    val rc = openArray(arrayName, mode)
    if (rc != TileDBErrorCode.TILEDB_OK) {
      new TileDBErrorCode(rc.toInt)
    }

    val csvFileName = workspaces(0) + "/__tiledb_spark." + arrayName + "." + rddId + ".csv"

    try {
      val file = new File(csvFileName)
      val bw = new BufferedWriter(new FileWriter(file))

      for (i <- cells) {
        bw.write(cells.toString)
      }

      bw.close()
    } catch {
      case ex: FileNotFoundException => println(csvFileName + ": No such file found")
      case ex: IOException => println("Had an IOException trying to read file " + csvFileName)
    }
    closeArray(arrayName)
  }

  /* Close method to release IO handlers before object deletion */
  override def close(): Unit = {
    for (context <- tileDBContexts) {
      new TileDBErrorCode(finalize_context(context))
    }
  }
}
