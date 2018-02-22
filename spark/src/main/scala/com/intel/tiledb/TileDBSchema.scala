/**
 * [[com.intel.tiledb.TileDBSchema]]
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
 * Manages the TileDB array schema by calling
 * the native [[../cpp/src/ArraySchema]] object
 */

package com.intel.tiledb

import com.intel.tiledb.datatypes.{TileDBAttribute, TileDBDataType, TileDBDimension}
import com.intel.tiledb.ordering.{CellOrder, TileOrder}
import org.apache.spark.Logging

/**
 * An array schema contains -
 * 1. Ordered list of dimensions. Dimensions have labels, ranges and types
 * 2. Ordered list of attributes. Attributes have labels and types
 * 3. Ordering of cells
 * 4. Ordering of tiles
 *
 * @param arrayName       Name of the array
 * @param dimTypeAsString Dimension type of the array
 * @param dimensionArray  Names of the dimensions in the array schema
 * @param attributes      Names of the attributes in the array schema
 * @param cellOrder       Order of the cells: Row-major, Column-major or Hilbert
 * @param cellSize        Size of one tile
 */
private [tiledb] class TileDBSchema(
  val arrayName: String,
  val dimTypeAsString: String,
  val dimensionArray: Array[(String, (Double, Double))],
  val attributes: Array[TileDBAttribute],
  val hasIrregularTiles: Boolean,
  val cellOrder : CellOrder.Value = CellOrder.COL_MAJOR,
  val cellSize: Int = 6,
  val tileOrder : TileOrder.Value = TileOrder.COL_MAJOR,
  val consolidationStep: Int = 5) extends Serializable with Logging {

  def getDimensionType = dimTypeAsString

  @transient
  val dimensions = {
    dimTypeAsString match {
      case TileDBDataType.CHAR =>
        // TODO: Handle byte type dimensions
        logError("Byte type dimensions are not supported")
        null
      case TileDBDataType.INT =>
        dimensionArray.map(d => {
          new TileDBDimension[Int](d._1, d._2._1.toInt, d._2._2.toInt)
        })
      case TileDBDataType.INT64 =>
        dimensionArray.map(d => {
          new TileDBDimension[Long](d._1, d._2._1.toLong, d._2._2.toLong)
        })
      case TileDBDataType.FLOAT =>
        dimensionArray.map(d => {
          new TileDBDimension[Float](d._1, d._2._1.toFloat, d._2._2.toFloat)
        })
      case TileDBDataType.DOUBLE =>
        dimensionArray.map(d => {
          new TileDBDimension[Double](d._1, d._2._1, d._2._2)
        })
      case x =>
        throw new IllegalArgumentException("Invalid dimension type found for array: " + arrayName)
    }
  }

  /** Convert array schema to a CSVLine
   * (duplicated from TileDB::array_schema.cc)
   */
  def toCSV : String = {
    //    val schemaAsCSV = "KARR2,1,attr1,2,i,j,0,1e+06,0,1e+06,float:1,int64,*,row-major,*,1000,5"
    var schemaAsCSV = collection.mutable.ArrayBuffer[String]()
    schemaAsCSV += arrayName
    schemaAsCSV += attributes.length.toString
    for (a <- attributes)
      schemaAsCSV += a.name
    schemaAsCSV += dimensions.length.toString
    for (d <- dimensions)
      schemaAsCSV += d.dimName
    for (d <- dimensions) {
      schemaAsCSV += d.lo.toString
      schemaAsCSV += d.hi.toString
    }
    for (a <- attributes) {
      schemaAsCSV += a.getTypeAsString
    }
    schemaAsCSV += dimTypeAsString
    // Print extent of a regular tile
    if (tileOrder == TileOrder.IRREGULAR) {
      schemaAsCSV += "*"
    } else {
      // TODO: Logic to get tile extents from schema
      // TODO: Test should break
    }
    schemaAsCSV += CellOrder.toString(cellOrder)
    schemaAsCSV += TileOrder.toString(tileOrder)
    schemaAsCSV += cellSize.toString
    schemaAsCSV += consolidationStep.toString
    schemaAsCSV.mkString(",")
  }

  override def toString: String = toCSV

  /**
   * Return true if the attribute is of variable length
   *
   * @param attributeId  id of the attribute
   * @return
   */
  def isVarAttribute(attributeId: Int): Boolean = attributes(attributeId).isVariableAttribute


  /**
   * Get the length of a variable attribute
   *
   * @param id  Attribute id
   * @return    Length of the attribute
   */
  def getAttributeLength(id: Int) = {
    attributes(id).getLength
  }

  def getAttributeType(id: Int) : String = {
    attributes(id).getTypeAsString
  }

  /**
   * Get attribute names
   * @return
   */
  def getAttributeNames: Array[String] = {
    val a = new Array[String](attributes.length)
    for ((x, i) <- attributes.zipWithIndex) {
      a(i) = x.name
    }
    a
  }

  /**
   * Get the dimension names
   */
  def getDimensionNames: Array[String] = {
    val d = new Array[String](dimensions.length)
    for ((x, i) <- dimensions.zipWithIndex) {
      d(i) = x.dimName
    }
    d
  }

  def getNumDimensions = dimensionArray.length

  def getNumAttributes = attributes.length

  import util.control.Breaks._

  /**
    * For a given set of attribute names, get the corresponding
    * ids
    *
    * @param attributeNames  Array containing the names of the attributes
    * @return
    */
  def getAttributeIds(attributeNames: Array[String]): Array[Int] = {

    if (attributeNames == null) return Array.empty
    if (attributeNames.isEmpty) return Array.empty

    val attrIds = new Array[Int](attributeNames.length)
    var idx = 0

    for (attrName <- attributeNames) {
      for ((x, i) <- attributes.zipWithIndex) {
        breakable {
          if (x.name.equals(attrName)) {
            attrIds(idx) = i
            idx += 1
            break()
          }
        }
      }
    }
    attrIds
  }

  /**
   * For a given list of dimension names, return the ids
   *
   * @param dimensionNames  Names of the array dimensions
   * @return
   */
  def getDimensionIds(dimensionNames: Array[String]): Array[Int] = {

    var names = dimensionNames
    if (dimensionNames == null || dimensionNames.isEmpty) {
      names = getDimensionNames
    }

    val dimIds = new Array[Int](names.length)
    var idx = 0
    for (dimName <- names) {
      for ((x, i) <- dimensions.zipWithIndex) {
        if (dimName.equals(x.dimName)) {
          dimIds(idx) = i
          idx += 1
        }
      }
    }
    dimIds
  }
}

object TileDBSchema {

  /**
   * Deserialization logic of array schema duplicated from
   * $TILEDB_HOME/src/misc/array_schema.cc:deserialize_csv()
   *
   * @param arrayName   name of the array
   * @param schemaAsCSV the schema of the array in CSV format
   * @return            TileDBSchema object in Scala
   */

  def apply(arrayName: String, schemaAsCSV: String): TileDBSchema = {
    val items = schemaAsCSV.split(",")
    val arrayName = items(0)
    val attributeSize = items(1).toInt
    val attributeNames = new Array[String](attributeSize)
    var index = 2

    for (i <- 0 until attributeSize) {
      attributeNames(i) = items(index)
      index += 1
    }
    val dimensionSize = items(index).toInt
    index += 1

    val dimensions = new Array[(String, (Double, Double))](dimensionSize)
    val dimensionNames = new Array[String](dimensionSize)
    val dimRangesAsString = new Array[(String, String)](dimensionSize)

    for (i <- 0 until dimensionSize) {
      dimensionNames(i) = items(index)
      index += 1
    }
    for (i <- 0 until dimensionSize) {
      dimRangesAsString(i) = (items(index), items(index + 1))
      index += 2
    }
    val attributes = new Array[TileDBAttribute](attributeSize)
    for (i <- 0 until attributeSize) {
      val at = items(index)
      val s = at.split(":")
      val isVarAttr = if (s(1).equals("var")) true else false
      attributes(i) = new TileDBAttribute(attributeNames(i),
                            s(0), isVarAttr, if (isVarAttr) Int.MaxValue else s(1).toInt)
      index += 1
    }
    val dimensionType = items(index)
    for (i <- 0 until dimensionSize) {
      dimensions(i) = (dimensionNames(i), (dimRangesAsString(i)._1.toDouble,
        dimRangesAsString(i)._2.toDouble))
    }
    index += 1
    val hasIrregularTiles = if (items(index).equals(SCHEMA_DEFAULT_STAR)) true else false
    index += 1
    val cellOrder = CellOrder(items(index))
    index += 1
    val tileOrder = TileOrder(items(index))
    index += 1
    val capacity = if (items(index).equals(SCHEMA_DEFAULT_STAR)) 0 else items(index).toInt
    index += 1
    val consolidationStep = items(index).toInt

    new TileDBSchema(arrayName, dimensionType, dimensions.toArray,
      attributes, hasIrregularTiles, cellOrder, capacity, tileOrder, consolidationStep)
  }

  /* Note that "*" means different thing
   * for attributes, tile order, cell order and size
   */
  val SCHEMA_DEFAULT_STAR = "*"
}