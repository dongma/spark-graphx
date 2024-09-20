package rdd.transfrom

import org.apache.spark.sql.SparkSession

import scala.collection._

/**
 * Created by madong on 2024/9/20.
 */
object I2ITransformations {

  val spark = SparkSession.builder()
    .appName("I2I Transformation")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  /*
   Science Project:
   each metric has identify, value
   Return the smallest ("best") 10 metrics (identifiers + values)
   */
//  Generator.generateMetrics("example-data/rtjvm/generated/metrics10m.txt", 10000000)

  val LIMIT = 10
  def readMetrics() = sc.textFile("example-data/rtjvm/generated/metrics10m.txt")
    .map { line =>
      val tokens = line.split(" ")
      val name = tokens(0)
      val value = tokens(1)
      (name, value.toDouble)
    }

  def printTopMetrics() = {
    val sortedMetrics = readMetrics().sortBy(_._2).take(LIMIT)
    sortedMetrics.foreach(println)
  }

  def printTopMetricsI2I() = {
    val topMetrics = readMetrics()
      .mapPartitions { records: Iterator[(String, Double)] =>
        /*
         i2i transformation
         - they are NARROW TRANSFORMATIONS
         - Spark will "selectively" spill data to disk when partitions are too big for memory
         Warning: don't traverse more than once or convert to collections
         */
        implicit val ordering: Ordering[(String, Double)] = Ordering.by[(String, Double), Double](_._2)
        val limitedCollection = new mutable.TreeSet[(String, Double)]()

        records.foreach { record =>
          limitedCollection.add(record)
          if (limitedCollection.size > LIMIT) {
            limitedCollection.remove(limitedCollection.last)
          }
        }
        // I've traversed the iterator
        limitedCollection.iterator
      }
      .repartition(1)
      .mapPartitions { records: Iterator[(String, Double)] =>
        /*
         i2i transformation
         - they are NARROW TRANSFORMATIONS
         - Spark will "selectively" spill data to disk when partitions are too big for memory
         Warning: don't traverse more than once or convert to collections
         */
        implicit val ordering: Ordering[(String, Double)] = Ordering.by[(String, Double), Double](_._2)
        val limitedCollection = new mutable.TreeSet[(String, Double)]()

        records.foreach { record =>
          limitedCollection.add(record)
          if (limitedCollection.size > LIMIT) {
            limitedCollection.remove(limitedCollection.last)
          }
        }
        // I've traversed the iterator
        limitedCollection.iterator
      }

    val result = topMetrics.take(LIMIT)
    result.foreach(println)
  }

  /**
   * Exercises
   */
  def printTopMetricsEx1() = {
    /*
      Better than the "dummy" approach - not sorting the entire RDD
      Bad (worse than the optional)
      - sorting the entire partition
      - forcing the iterator in memory - this can cause OOM your executors
     */
    val topMetrics = readMetrics()
      .mapPartitions(_.toList.sortBy(_._2).take(LIMIT).toIterator)
      .repartition(1)
      .mapPartitions(_.toList.sortBy(_._2).take(LIMIT).toIterator)
      .take(LIMIT)
    topMetrics.foreach(println)
  }

  /*
   Better than ex1
   - extracting top 10 values per partition instead of sorting the entire partition
   Bad because
   - forcing toList can OOM your executors
   - iterating over the list twice
   - if the list is immutable, time spent allocating objects and GC
   */
  def printTopMetricsEx2() = {
    // ~~ the code is same to printTopMetricsI2I method
  }

  def main(args: Array[String]): Unit = {
    printTopMetrics()
    printTopMetricsI2I()
    Thread.sleep(1000000)
  }

}
