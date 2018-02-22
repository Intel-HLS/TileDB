/**
 * [[com.intel.tiledb.TileDBConfSpec]]
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
 * Tests for TileDB configuration
 */

package com.intel.tiledb

import org.apache.spark.SparkConf
import org.scalatest.FunSuite

class TileDBConfSpec extends FunSuite {

  test("TileDB configuration is able to load system properties") {
    System.setProperty("spark.tiledb.test.testProperty", "2")
    val conf = new SparkConf()
    assert(conf.get("spark.tiledb.test.testProperty") === "2")
  }
}
