/**
 * [[com.intel.tiledb.codes.TileDBErrorCode]]
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
 * TileDB error codes duplicated from $TILEDB_HOME/core/src/capis/tiledb_error.cc
 */

package com.intel.tiledb.codes

import java.io.IOException

private [tiledb] class TileDBErrorCode(code: Int) {

  import com.intel.tiledb.codes.TileDBErrorCode._
  code match {
    case TILEDB_OK =>
    case TILEDB_EPARSE => throw new IOException(TILEDB_EPARSE_STR)
    case TILEDB_ENDEFARR => throw new IOException(TILEDB_ENDEFARR_STR)
    case TILEDB_EFILE => throw new IOException(TILEDB_EFILE_STR)
    case TILEDB_ENSMCREAT => throw new IOException(TILEDB_ENSMCREAT_STR)
    case TILEDB_ENLDCREAT => throw new IOException(TILEDB_ENLDCREAT_STR)
    case TILEDB_ENQPCREAT => throw new IOException(TILEDB_ENQPCREAT_STR)
    case TILEDB_EINIT => throw new IOException(TILEDB_EINIT_STR)
    case TILEDB_EFIN => throw new IOException(TILEDB_EFIN_STR)
    case TILEDB_EPARRSCHEMA => throw new IOException(TILEDB_EPARRSCHEMA_STR)
    case TILEDB_EDNEXIST => throw new IOException(TILEDB_EDNEXIST_STR)
    case TILEDB_EDNCREAT => throw new IOException(TILEDB_EDNCREAT_STR)
    case TILEDB_ERARRSCHEMA => throw new IOException(TILEDB_ERARRSCHEMA_STR)
    case TILEDB_EDEFARR => throw new IOException(TILEDB_EDEFARR_STR)
    case TILEDB_EOARR => throw new IOException(TILEDB_EOARR_STR)
    case TILEDB_ECARR => throw new IOException(TILEDB_ECARR_STR)
    case TILEDB_EIARG => throw new IOException(TILEDB_EIARG_STR)
  }
}

object TileDBErrorCode {
  val TILEDB_OK = 0
  val TILEDB_OK_STR = "No error"

  val TILEDB_EPARSE = -1
  val TILEDB_EPARSE_STR = "Parser error"

  val TILEDB_ENDEFARR = -2
  val TILEDB_ENDEFARR_STR = "Undefined array"

  val TILEDB_EFILE = -3
  val TILEDB_EFILE_STR = "File operation failed"

  val TILEDB_ENSMCREAT = -4
  val TILEDB_ENSMCREAT_STR = "Failed to create storage manager"

  val TILEDB_ENLDCREAT = -5
  val TILEDB_ENLDCREAT_STR = "Failed to create loader"

  val TILEDB_ENQPCREAT = -6
  val TILEDB_ENQPCREAT_STR = "Failed to create query processor"

  val TILEDB_EINIT = -7
  val TILEDB_EINIT_STR = "Failed to initialize TileDB"

  val TILEDB_EFIN = -8
  val TILEDB_EFIN_STR = "Failed to finalize TileDB"

  val TILEDB_EPARRSCHEMA = -9
  val TILEDB_EPARRSCHEMA_STR = "Failed to parse array schema"

  val TILEDB_EDNEXIST = -10
  val TILEDB_EDNEXIST_STR = "Directory does not exist"

  val TILEDB_EDNCREAT = -11
  val TILEDB_EDNCREAT_STR = "Failed to create directory"

  val TILEDB_ERARRSCHEMA = -12
  val TILEDB_ERARRSCHEMA_STR = "Failed to retrieve array schema"

  val TILEDB_EDEFARR = -13
  val TILEDB_EDEFARR_STR = "Failed to define array"

  val TILEDB_EOARR = -14
  val TILEDB_EOARR_STR = "Failed to open array"

  val TILEDB_ECARR = -15
  val TILEDB_ECARR_STR = "Failed to close array"

  val TILEDB_EIARG = -16
  val TILEDB_EIARG_STR = "Invalid argument"

  /** Check is code is an error or not */
  def isError(code: Int): Boolean = {
    code match {
      case TILEDB_EPARSE | TILEDB_ENDEFARR |
           TILEDB_EFILE | TILEDB_ENSMCREAT | TILEDB_ENLDCREAT |
           TILEDB_ENQPCREAT | TILEDB_EINIT | TILEDB_EFIN |
           TILEDB_EPARRSCHEMA | TILEDB_EDNEXIST | TILEDB_EDNCREAT |
           TILEDB_ERARRSCHEMA | TILEDB_EDEFARR | TILEDB_EOARR |
           TILEDB_ECARR | TILEDB_EIARG => true
      case TILEDB_OK => false
      case _ => false
    }
  }

  def apply(code: Int) = {
    new TileDBErrorCode(code)
  }

}
