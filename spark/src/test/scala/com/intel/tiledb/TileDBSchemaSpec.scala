/**
 * [[com.intel.tiledb.TileDBSchemaSpec]]
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
 * Contains test for TileDBSchema in Scala
 *
 */

package com.intel.tiledb

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.sys.process._

class TileDBSchemaSpec extends FunSuite with BeforeAndAfterAll {

  var workspaces: Array[String] = null
  val arrayHilbertName = "TEST_HILBERT_ARRAY"
  val arrayColMajorName = "TEST_COLMAJOR_ARRAY"
  val arrayRowMajorName = "TEST_ROWMAJOR_ARRAY"
  val nDimensions = 2
  val nAttributes  = 5
  val attributeLength = 4

  override def beforeAll(): Unit = {

    val ws = System.getenv("TILEDB_WORKSPACE")
    val tileDBHome = System.getenv("TILEDB_HOME")
    val tileDBDefineArrayCmd = tileDBHome + "/tiledb_cmd/bin/release/tiledb_define_array"

    if (ws == null || ws.isEmpty)
      throw new IllegalArgumentException("Please rerun test with TILEDB_WORKSPACE")
    else
      workspaces = ws.split(",")

    var cmd = Seq(
      tileDBDefineArrayCmd, "-w", ws,
      "-A", arrayHilbertName, "-a", "attr0,attr1,attr2,attr3,attr4", "-d", "i,j",
      "-D", "0,100,0,100", "-t",
      "double:1,int:2,char:3,int64:var,float:" + attributeLength + ",int64", "-c",
      "1000", "-s", "5", "-o", "hilbert")
    cmd.lines

    cmd = Seq(tileDBDefineArrayCmd, "-w", ws,
      "-A", arrayColMajorName, "-a", "attr1", "-d", "i,j",
      "-D", "0,100,0,100", "-t", "float:1,int64", "-c",
      "1000", "-s", "5", "-o", "column-major")
    cmd.lines

    cmd = Seq(tileDBDefineArrayCmd, "-w", ws,
      "-A", arrayRowMajorName, "-a", "attr1", "-d", "i,j",
      "-D", "0,100,0,100", "-t", "float:1,int64", "-c",
      "1000", "-s", "5", "-o", "row-major")
    cmd.lines

  }

  test("basic TileDB schema operations") {
    val schemaHilbert = arrayHilbertName + "," + nAttributes +
      "," + "attr0,attr1,attr2,attr3,attr4,"+ nDimensions +
      ",i,j,0,100,0,100,double:1,int:2,char:3,int64:var,float:" +
      attributeLength + ",int64,*,hilbert,*,1000,5"
    val tsHilbert = TileDBSchema(arrayHilbertName, schemaHilbert)
    assert(tsHilbert.toString().contains(schemaHilbert))
    val aNames = tsHilbert.getAttributeNames
    assert(aNames(0).equals("attr0"))
    assert(aNames(1).equals("attr1"))
    assert(aNames(2).equals("attr2"))
    assert(aNames(3).equals("attr3"))
    assert(aNames(4).equals("attr4"))
    assert(tsHilbert.getNumDimensions == nDimensions)
    assert(tsHilbert.getNumAttributes == nAttributes)
    assert(tsHilbert.getDimensionType.equals("int64"))
    assert(tsHilbert.getAttributeType(0).equals("double:1"))
    assert(tsHilbert.getAttributeType(1).equals("int:2"))
    assert(tsHilbert.getAttributeType(2).equals("char:3"))
    assert(tsHilbert.getAttributeType(3).equals("int64:var"))
    assert(tsHilbert.getAttributeType(4).equals("float:" + attributeLength))
    assert(!tsHilbert.isVarAttribute(0))
    assert(!tsHilbert.isVarAttribute(1))
    assert(!tsHilbert.isVarAttribute(2))
    assert(tsHilbert.isVarAttribute(3))
    assert(!tsHilbert.isVarAttribute(4))
    assert(tsHilbert.getAttributeLength(0) == 1)
    assert(tsHilbert.getAttributeLength(1) == 2)
    assert(tsHilbert.getAttributeLength(2) == 3)
    assert(tsHilbert.getAttributeLength(3) == Int.MaxValue)
    assert(tsHilbert.getAttributeLength(4) == attributeLength)

    val schemaColMajor = arrayColMajorName + "," + 1 +
      "," + "attr1,2,i,j,0,100,0,100,float:1,int64,*,column-major,*,1000,5"
    val tsColMajor = TileDBSchema(arrayColMajorName, schemaColMajor)
    assert(tsColMajor.toString().contains(schemaColMajor))

    val schemaRowMajor = arrayRowMajorName + "," + 1 +
      "," + "attr1,2,i,j,0,100,0,100,float:1,int64,*,row-major,*,1000,5"
    val tsRowMajor = TileDBSchema(arrayRowMajorName, schemaRowMajor)
    assert(tsRowMajor.toString().contains(schemaRowMajor))

    val schemaDoubleDimType = arrayRowMajorName + "," + 1 +
      "," + "attr1,2,i,j,0.0,100.0,0.0,100.0,float:1,double,*,row-major,*,1000,5"
    val tsDD = TileDBSchema(arrayRowMajorName, schemaDoubleDimType)
    assert(tsDD.toString().contains(schemaDoubleDimType))

    val schemaFloatDimType = arrayRowMajorName + "," + 1 +
      "," + "attr1,2,i,j,0.0,100.0,0.0,100.0,float:1,float,*,row-major,*,1000,5"
    val tsDF = TileDBSchema(arrayRowMajorName, schemaFloatDimType)
    assert(tsDF.toString().contains(schemaFloatDimType))

    val schemaIntDimType = arrayRowMajorName + "," + 1 +
      "," + "attr1,2,i,j,0,100,0,100,float:1,int,*,row-major,*,1000,5"
    val tsDI = TileDBSchema(arrayRowMajorName, schemaIntDimType)
    assert(tsDI.toString().contains(schemaIntDimType))

    val schemaInvalidDimType = arrayRowMajorName + "," + 1 +
      "," + "attr1,2,i,j,0,100,0,100,float:1,asdasd,*,row-major,*,1000,5"
    intercept[IllegalArgumentException] {
      val tsDInv = TileDBSchema(arrayRowMajorName, schemaInvalidDimType)
    }
    assert(true)
  }

  override def afterAll(): Unit = {
    // Do nothing
  }
}
