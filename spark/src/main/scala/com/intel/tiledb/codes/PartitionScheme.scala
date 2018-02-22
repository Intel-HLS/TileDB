package com.intel.tiledb.codes

object PartitionScheme extends Enumeration {
  type PartitionScheme = Value
  val ROW_MAJOR, COL_MAJOR, HILBERT = Value

  def apply(order: String): PartitionScheme = {
    order match {
      case "row-major" => ROW_MAJOR
      case "column-major" => COL_MAJOR
      case "hilbert" => HILBERT
    }
  }

  def toString(order: PartitionScheme) = {
    order match {
      case ROW_MAJOR => "row-major"
      case COL_MAJOR => "column-major"
      case HILBERT => "hilbert"
    }
  }
}
