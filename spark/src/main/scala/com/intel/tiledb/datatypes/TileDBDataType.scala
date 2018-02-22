package com.intel.tiledb.datatypes

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.types.DataType

object TileDBDataType {
  val INT     = "int"
  val INT64   = "int64"
  val DOUBLE  = "double"
  val FLOAT   = "float"
  val CHAR    = "char"
  val INVALID = ""

  def isValid(that: String): Boolean = {
    that.equals(INT) || that.equals(INT64) ||
    that.equals(FLOAT) || that.equals(DOUBLE) ||
      that.equals(CHAR)
  }
}

/** 64-bit signed integer type */
@DeveloperApi
case class Int64Type(in: Int) extends DataType {
  override def defaultSize: Int = 64
  def asNullable: Int64Type = this
}