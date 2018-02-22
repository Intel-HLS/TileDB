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
 * The main class holding cell payload and coordinates from TileDB
 */

package com.intel.tiledb

import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * This class holds the native TileDB cells. In this implementation
 * both parsed and native cells are maintained which can cause
 * memory leaks. Needs fixing.
 *
 * @param dimValues        Dimension values of the cell
 * @param attrValues       Attribute values of the cell
 * @tparam D               Dimension type
 */
class TileDBCell[D: ClassTag](
  val filterDims: Array[String],
  val filterAttrs: Array[String],
  val dimValues: Array[String] = Array.empty,
  val attrValues: Array[String] = Array.empty,
  val cellAsCSV: String,
  val schema: TileDBSchema)
  extends Cloneable with Serializable with Logging {

  var dimOffsets = HashMap[String, Int]()
  var attrOffsets = HashMap[String, Int]()
  if (filterDims == null || filterDims.isEmpty) {
    val dimnames = schema.getDimensionNames
    for ((d,i) <- dimnames.zipWithIndex) dimOffsets += d->i
  } else {
    for ((d,i) <- filterDims.zipWithIndex) dimOffsets += d->i
  }

  if (filterAttrs == null || filterAttrs.isEmpty) {
    val attrnames = schema.getAttributeNames
    for ((d,i) <- attrnames.zipWithIndex) {
      attrOffsets += d->i
      logDebug("added attribute: " + d)
    }
  } else {
    for ((d,i) <- filterAttrs.zipWithIndex) {
      attrOffsets += d->i
      logDebug("added attribute: " + d)
    }
  }

  /**
   * Clones a TileDB cell
   * 
   * @param that   LHS TileDB cell
   * @return       A clone of the given TileDB cell
   */
  def clone(that: TileDBCell[D]) : TileDBCell[D] = {
    if (that.dimValues.isEmpty && that.attrValues.isEmpty)
      TileDBCell.empty
    new TileDBCell[D](filterDims,
      filterAttrs, that.dimValues, that.attrValues, that.cellAsCSV, schema)
  }

  override def toString: String = {
    if (dimValues.isEmpty && attrValues.isEmpty) ""
    else
      dimValues.mkString(",") + "," + attrValues.mkString(",")
//    cellAsCSV
  }

  def getDimValue(dimName: String): Double = {
    dimValues(dimOffsets(dimName)).toDouble
  }

  def getAttrVal(attrName: String): Double = {
    attrValues(attrOffsets(attrName)).toDouble
  }

  def getValues(names: Array[String]): Array[Double] = {
    val vector = Array[Double](names.length)
    for (obj <- names) {
      if (dimOffsets.contains(obj)) {
        vector :+ dimOffsets(obj).toDouble
      } else if (attrOffsets.contains(obj)) {
        vector :+ attrOffsets(obj).toDouble
      }
      else vector :+ 0.0
    }
    vector
  }

  def toLabeledPoint(label: String, features: Array[String]): LabeledPoint = {
    LabeledPoint(getAttrVal(label), Vectors.dense(getValues(features)))
  }
}

object TileDBCell {

  def empty[D: ClassTag]: TileDBCell[D] = {
    new TileDBCell[D](Array.empty, Array.empty, Array.empty, Array.empty, "", null)
  }

  def apply[D: ClassTag](cellAsCSV: String, schema: TileDBSchema): TileDBCell[D] = {
//    new TileDBCell[D](Array.empty, Array.empty, cellAsCSV)
    parse[D](cellAsCSV, schema)
  }

  def apply[D: ClassTag](cellAsCSV: String, schema: TileDBSchema,
                          filterDims: Array[String],
                          filterAttrs: Array[String]): TileDBCell[D] = {
    parse[D](cellAsCSV, schema, filterDims, filterAttrs)
  }

  /**
   * Parse a TileDB cell (as a string) to a typed structure as defined by the
   * array schema.
   *
   * @param cellAsCSV  Raw cell
   * @return           No return, populates the TileDBCell internal structures
   */
  def parse[D: ClassTag](
    cellAsCSV: String,
    schema: TileDBSchema,
    filterDims: Array[String] = null,
    filterAttrs: Array[String] = null) : TileDBCell[D] = {

    if (cellAsCSV.isEmpty) {
      throw new IllegalArgumentException("Empty cell found in TileDB instance")
    }

    val nDimensions = schema.getNumDimensions
    val nAttributes = schema.getNumAttributes

    val dimValues = new Array[String](nDimensions)
    val attrValues = new ArrayBuffer[String]()
    val elements = cellAsCSV.split(",")
    var count: Int = 0

    for (i <- 0 until nDimensions) {
      dimValues(i) = elements(count)
      count += 1
    }

    for (i <- 0 until nAttributes) {
      if (schema.isVarAttribute(i)) {
        if (schema.getAttributeType(i).equals("char")) {
          count += 1
          attrValues += elements(count)
          count += 1
        } else {
          val size = elements(count).toInt
          count += 1
          val attributes = new Array[String](size)

          for (j <- 0 until size) {
            attributes(j) = elements(count)
            count += 1
          }
          attrValues += attributes.mkString(",")
        }
      } else {
        for (j <- 0 until schema.getAttributeLength(i)) {
          attrValues += elements(count)
          count += 1
        }
      }
    }

    new TileDBCell[D](filterDims, filterAttrs, dimValues, attrValues.toArray, cellAsCSV, schema)
  }
}
