/**
 * [[com.intel.tiledb.datatypes.TileDBAttribute]]
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
 * Stores the name, data type, length and operations of an TileDB attribute
 * object
 */

package com.intel.tiledb.datatypes

import org.apache.spark.Logging

class TileDBAttribute(
  val name: String,
  val attrType: String,
  val isVariableAttribute: Boolean,
  val length: Int) extends Serializable with Cloneable with Logging {

  def clone(that: TileDBAttribute): TileDBAttribute = {
    new TileDBAttribute(that.name, that.attrType, that.isVariableAttribute, that.length)
  }

//  def getTypeAsClass: Class[_ <: DataType] = {
//    attrType match {
//      case TileDBDataType.INT => classOf[IntegerType]
//      case TileDBDataType.INT64 => classOf[Int64Type]
//      case TileDBDataType.DOUBLE => classOf[DoubleType]
//      case TileDBDataType.FLOAT => classOf[FloatType]
//      case TileDBDataType.CHAR => classOf[ByteType]
//    }
//  }

  def getLength = length

  /**
   * Type to string logic duplicated from $TILEDB_HOME/src/misc/array_schema.cc
   * @return
   */
  def getTypeAsString: String = attrType + ":" + (if (length == Int.MaxValue) "var" else length)

  override def toString: String = {
    name + "=" + attrType + ":" + (if (length == Int.MaxValue) "var" else length)
  }
}
