/**
 * [[com.intel.tiledb.ordering.CellOrder]]
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
 * The values of cell order
 */

package com.intel.tiledb.ordering

object CellOrder extends Enumeration {
  type CellOrder = Value
  val ROW_MAJOR, COL_MAJOR, HILBERT = Value

  def apply(order: String): CellOrder = {
    order match {
      case "row-major" => ROW_MAJOR
      case "column-major" => COL_MAJOR
      case "hilbert" => HILBERT
    }
  }

  def toString(order: CellOrder) = {
    order match {
      case ROW_MAJOR => "row-major"
      case COL_MAJOR => "column-major"
      case HILBERT => "hilbert"
    }
  }
}