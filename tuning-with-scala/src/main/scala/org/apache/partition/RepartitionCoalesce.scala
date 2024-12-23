package org.apache.partition

import org.apache.spark.sql.SparkSession

/**
 * spark repartition分区
 *
 * @author Sam Ma
 * @date 2024/12/23
 */
object RepartitionCoalesce {

  val spark = SparkSession.builder().appName("Repartition And Coalesce")
    .master("local[*]")
    .getOrCreate()
  val sc = spark.sparkContext

  val numbers = sc.parallelize(1 to 10000000)
  println(numbers.partitions.length)  // number of virtual cores

  // repartition
  val repartitionNumbers = numbers.repartition(2)
  repartitionNumbers.count()

  // coalesce - fundamentally different
  val coalesceNumbers = numbers.coalesce(2) // for a smaller number of partitions
  coalesceNumbers.count()

  // force coalesce to be a shuffle，对coalesce进行强制partition
  val forcedShuffleNumbers = numbers.coalesce(2, true)  // force a shuffle

  def main(args: Array[String]): Unit = {
    Thread.sleep(10000000)
  }

}
