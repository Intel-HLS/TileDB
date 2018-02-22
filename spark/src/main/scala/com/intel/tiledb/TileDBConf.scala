/**
 * [[com.intel.tiledb.TileDBConf]]
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
 */

package com.intel.tiledb

import org.apache.spark.{Logging, SparkConf}

import scala.collection.JavaConversions._
import scala.collection.Map

class TileDBConf(val loadDefaults: Boolean, val sparkConf: SparkConf)
  extends Cloneable with Logging {

  import TileDBConf._

  /**
   * Not using ConcurrentHashMap as low degree of contention is expected
   * for a TileDB configuration object
   */
  @transient protected[tiledb] val settings = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, String]())

  /** Creates the default TileDB configuration */
  def this(sparkConf: SparkConf) = this(true, sparkConf)

  /** Create a copy from this */
  def this(that: TileDBConf) = {
    this(that.loadDefaults, that.sparkConf)
    this.setAll(that.getAll)
  }

  if (loadDefaults) {
    settings.put(TILEDB_SEGMENT_SIZE, TileDBArray.TILEDB_SEGMENT_SIZE_DEFAULT)
    settings.put(TILEDB_WRITE_STATE_MAX_SIZE, "100000000")
    settings.put(TILEDB_PARTITION_SIZE, 100.toString)
    // Load any spark.* system properties
    for ((key, value) <- getSystemProperties if key.startsWith("spark.tiledb.")) {
      set(key, value)
    }
    if (getConf(TILEDB_WORKSPACE,"").isEmpty) {
      throw new IllegalArgumentException("TILEDB_WORKSPACE must be set." +
        "Also check spark.tiledb.workspace.dir configuration parameter")
    }
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): TileDBConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    settings.put(key, value)
    this
  }

  /** Returns the system properties map that is thread-safe to iterator over. It gets the
    * properties which have been set explicitly, as well as those for which only a default value
    * has been defined. */
  def getSystemProperties: Map[String, String] = {
    val sysProps =
      for (key <- System.getProperties.stringPropertyNames()) yield
        (key, System.getProperty(key))

    sysProps.toMap
  }

  /**
   * Return the value of TileDB configuration property for the given key. If the key is not set
   * yet, return `defaultValue`
   */
  def getConf(key: String, defaultValue: String): String = {
    Option(settings.get(key)).getOrElse(defaultValue)
  }

  def getPartitionSize: Int = getConf(TILEDB_PARTITION_SIZE, defaultPartitionSize).toInt
  def setPartitionSize(size: Int) = set(TILEDB_PARTITION_SIZE, size.toString)

  /**
   * Return the TileDB workspace
   * @return
   */
  def getWorkspaces: Array[String] = {
    val wsList = getConf(TILEDB_WORKSPACE, "")
    wsList.split(",")
  }

  /**
   * Set the TileDB workspace with a given path
   * This inherently assumes there is only one
   * TileDB instance
   */
  def setWorkspace(path: String) : TileDBConf = set(TILEDB_WORKSPACE, path)

  def checkNumInstances(nWorkspaces: Int) = {
    if (nWorkspaces != getNumInstances) {
      val msg = "Number of workspaces does not match number of instances"
      logError(msg)
      throw new scala.IllegalArgumentException(msg)
    }
  }

  /**
   * Set the TileDB workspaces with variable
   * number of workspaces. Automatically checks
   * whether number of instances match. If not,
   * throws an illegal argument exception
   */
  def setWorkspaces(paths: String*): TileDBConf = {
    checkNumInstances(paths.size)
    set(TILEDB_WORKSPACE, paths.mkString(","))
  }

  /**
   * Set the TileDB workspaces Automatically checks
   * whether number of instances match. If not,
   * throws an illegal argument exception
   */
  def setWorkspaces(paths: Array[String]): TileDBConf = {
    checkNumInstances(paths.length)
    set(TILEDB_WORKSPACE, paths.mkString(","))
  }

  /**
   * Set number of TileDB instances
   *
   * @param nInstances  # of instances
   * @return
   */
  def setNumInstances(nInstances: Int): TileDBConf = setInt(TILEDB_NUM_INSTANCES, nInstances)

  /**
   * Get # of TileDB instances
   * @return
   */
  def getNumInstances: Int = getConf(TILEDB_NUM_INSTANCES, defaultNumInstances.toString).toInt

  /** Get all parameters as a list of pairs */
  def getAll : Array[(String, String)] = {
    settings.entrySet().map(x => (x.getKey, x.getValue)).toArray
  }

  def setInt(key: String, value: Int): TileDBConf = {
    set(key, value.toString)
  }

  /** Set multiple parameters together */
  def setAll(settings: Traversable[(String, String)]): TileDBConf = {
    settings.foreach { case (k, v) => set(k, v) }
    this
  }

  /** Copy this object */
  override def clone: TileDBConf = {
    new TileDBConf(this)
  }
}

private [tiledb] object TileDBConf {
  val TILEDB_WORKSPACE = "spark.tiledb.workspace.dir"
  val TILEDB_SEGMENT_SIZE = "spark.tiledb.segment.size"
  val TILEDB_WRITE_STATE_MAX_SIZE = "spark.tiledb.write.state.max.size"
  val TILEDB_PARTITION_SIZE = "spark.tiledb.partition.size"
  val TILEDB_NUM_INSTANCES = "spark.tiledb.num.instances"

  val defaultPartitionSize: String = "10"
  val defaultNumInstances: Int = 1
}

