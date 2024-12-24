package org.apache.partition

import org.apache.spark.{HashPartitioner, Partitioner, RangePartitioner}
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
 * 不同的partitioner分区，HashPartitioner、RangePartitioner、CustomPartitioner. 但跑的太慢了
 *
 * @author Sam Ma
 * @date 2024/12/24
 */
object Partitioners {

  val spark = SparkSession.builder().appName("partitioners")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  val numbers = sc.parallelize(1 to 100)
  println(numbers.partitioner)  // None

  val numbers3 = numbers.repartition(3) // random data redistribution
  println(numbers3.partitioner) // None

  // keep track the partitioner
  // KV RDDs can control the partitioning schema
  val keyedNumbers = numbers.map(n => (n % 10, n))  // RDD[(INT, INT)]
  val hashedNumbers = keyedNumbers.partitionBy(new HashPartitioner(4))  // keys with the same hash stay on the same partition
  /*
   keys with the same hash stay on the same partition
   Prerequisite for:
    - combineByKey
    - groupByKey
    - aggregateByKey
    - foldByKey
    - reduceByKey
   Prereq for joins, when neither RDD has a known partitioner
   */
  val rangeNumbers = keyedNumbers.partitionBy(new RangePartitioner(5, keyedNumbers))
  /*
   Keys with the same range will be on the same partitioner
   For a spectrum 0-1000:
    keys between Int.MinValue-200 => partition 0
    keys between 200-400 => partition 1
    keys between 400-600 => partition 2
    keys between 600-800 => partition 3
    keys between 800-Int.MaxValue => partition 4

   RangePartitioner is a prerequisite for a SORT.
   */
  rangeNumbers.sortByKey()  // NOT incur a shuffle


  // define your own partitioner
  def generateRandomWords(nWords: Int, maxLength: Int) = {
    val r = new Random()
    (1 to nWords).map(_ => r.alphanumeric.take(r.nextInt(maxLength)).mkString(""))
  }

  val randomWordsRDD = sc.parallelize(generateRandomWords(100, 10))
  // repartition this RDD by the words length == two words of the same length will be on the same partition
  // custom partition = counting the occurrence of 'z' in every word
  val zWordsRDD = randomWordsRDD.map(word => (word, word.count(_ == 'z')))  // RDD[(String,Int)]

  class ByLengthPartitioner(override val numPartitions: Int) extends Partitioner {
    override def getPartition(key: Any): Int = {
      key.toString.length % numPartitions
    }
  }

  val byLengthZWords = zWordsRDD.partitionBy(new ByLengthPartitioner(100))

  def main(args: Array[String]): Unit = {
    byLengthZWords.foreachPartition(_.foreach(println))
  }

}
