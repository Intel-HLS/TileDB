package com.intel.tiledb.datatypes

import scala.reflect.ClassTag

private[tiledb] class TileDBDimension[T: ClassTag](
  val dimName: String,
  val lo: T,
  val hi: T) extends Serializable {

  /* Returns the range of the dimension */
  def range = (lo, hi)

  override def toString: String = dimName.toString + "," + range.toString()
}
