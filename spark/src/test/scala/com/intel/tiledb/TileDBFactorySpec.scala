/**
 * [[com.intel.tiledb.TileDBFactorySpec]]
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
 * Test cases for TileDBFactory
 */

package com.intel.tiledb

import com.intel.tiledb.codes.TileDBIOMode
import org.scalatest.FunSuite

class TileDBFactorySpec extends FunSuite {

  val workspace = Array("/home/kdatta1/TileDB/tiledb-ws/")
  val validArrayName = "TEST_ARRAY_VALID"
  val array8 = "ARRAY_8"
  val invalidArrayName = "TEST_ARRAY_INVALID"

//  test("TileDBFactory open_array should open an existing TileDB array") {
//    val tf = new TileDBFactory(workspace)
//    assert(tf.openArray(validArrayName, TileDBIOMode.READ) != -1)
//  }
//
//  test("open_array should throw ArrayIndexOutOfBoundsException " +
//    " for an invalid array name " + invalidArrayName) {
//    intercept[ArrayIndexOutOfBoundsException] {
//      val tf = new TileDBFactory(workspace)
//      val array = tf.openArray(invalidArrayName, "r")
//    }
//  }
//
  test("Successful loadCells") {
    val tf = new TileDBFactory(workspace).initContexts(1)
    val arrayId = tf.openArray(array8, TileDBIOMode.READ)
    val schema = tf.getArraySchema(array8)
    val attributes = Array("attr1")
    val dimensions = Array("i", "j")
    var payload = new Array[TileDBCell[Long]](8)
    val count = tf.loadCells[Long](array8, schema, dimensions, attributes, 0, payload)
    assert(count == 8)
  }

//  test("Successful loadNCells") {
//    val tf = new TileDBFactory(workspace).initContexts(1)
//    val arrayId = tf.openArray(array8, TileDBIOMode.READ)
//    val schema = tf.getArraySchema(array8)
//    val attributes = Array("attr1")
//    var payload = new Array[TileDBCell[Long]](8)
//    val ret = tf.initialize_const_cell_iterator(arrayId, attributes)
//    assert(ret == TileDBErrorCode.TILEDB_OK)
//    val n = 4
//    val dimensions: Array[String] = Array("i", "j")
//    val count = tf.loadNCells[Long](array8, schema, dimensions, attributes, 0, n, payload)
//    assert(count == n)
//  }
}


