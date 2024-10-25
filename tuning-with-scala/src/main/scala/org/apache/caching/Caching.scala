package org.apache.caching

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * Spark Caching缓存，DISK、Memory、Serializable
 *
 * @author Sam Ma
 * @date 2024/10/24
 */
object Caching {

  val spark = SparkSession.builder().appName("Caching")
    .master("local").getOrCreate()
  val flightsDF = spark.read.option("inferSchema", "true")
    .json("example-data/rtjvm/flights")
  flightsDF.count()

  // simulate an "expensive" operation
  val orderedFlightsDF = flightsDF.orderBy("dist")

  // scenario: use this DF multiple times
  orderedFlightsDF.persist(
    // no argument = MEMORY_AND_DISK
    // StorageLevel.MEMORY_ONLY  // cache the DF in memory EXACTLY - CPU efficient, memory expensive
    // StorageLevel.DISK_OK  // cache the DF to DISK - CPU efficient and mem efficient. but slower
    StorageLevel.MEMORY_AND_DISK  // TODO:
  )
  orderedFlightsDF.cache().count()
  orderedFlightsDF.count()

  /*
   * without cache: sorted count about 0.1s
   */
  def main(args: Array[String]): Unit = {
    Thread.sleep(1000000)
  }

}
