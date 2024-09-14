package rdd.transfrom

import org.apache.spark.sql.SparkSession

import scala.io.Source
import scala.util.{Random, Using}

/**
 * Created by madong on 2024/9/9.
 */
object ByKeyFunctions {

  val spark = SparkSession.builder().appName("Skewed Joins")
    .master("local[*]")
    .getOrCreate()
  val sc = spark.sparkContext

  /*
   * Scenario: assume we have a dataset with (word, occurrences) which we obtained after scraping
   * a big document or website.
   * We want to aggregate and sum all values under a single map.
   */
  val words: Seq[String] = Using.resource(Source.fromFile("example-data/rtjvm/lipsum/words.txt")) {
    source => source.getLines().toSeq
  }

  // generated data
  val random = new Random
  val wordCounts = sc.parallelize(Seq.fill(2000000)(words(random.nextInt(words.length)), random.nextInt(1000)))

  // the most intuitive solution can be the most dangerous
  /*
    - it cause a shuffle so that data associated with one key stays on the same machine
    - it can cause memory errors if the data is skewed, i.e data associated to one key has disproportination
      and may not fit in the executor memory.
   */
  val totalCounts = wordCounts
    .groupByKey() // RDD of key = word, value = iterable of all previous values
    .mapValues(_.sum)

  // ^^ 6s for 2M laptop sales [maybe adjust numbers]
  // ^^ look at the shuffle write - it shuffles the entire data
  totalCounts.collectAsMap()

  // TODO: reduceByKey、foldByKey、aggregateByKey、combineByKey
  /*
   ReduceByKey is the simplest - like a collection Also faster and safer because
   - it does a partial aggregate on the executor (operations done on the executors
   without shuffling are called map-side)
   - avoid the data skew problem
   - shuffle much less data
   */
  val totalCountsReduce = wordCounts.reduceByKey(_ + _)
  totalCountsReduce.collectAsMap()

  /*
   FoldByKey is similar to the collection fold function, Similar performance
   - need a 0 as value to start with
   - need a combination function
   */
  val totalCountsFold = wordCounts.foldByKey(0)(_ + _)
  totalCountsFold.collectAsMap()

  /*
   Aggregate by key is the more general and need a zero value 0 and 2 combination functions
   - one that combines the current aggregated value with a new element
   - ont that combines two aggregated values from different executors
   */
  val totalCountsAggregate = wordCounts.aggregateByKey(0.0)(_ + _, _ + _)
  totalCountsAggregate.collectAsMap()

  /*
  CombineByKey is the most general function available that can combine values inside your RDD, you need
  - a function that turns a value into aggregate value so that the further aggregates can start from it
  - a function to combine a current aggreate with a value in the RDD inside the executor
  - a function to combine 2 aggregates between executors
  - a number of partitions, or a partitioner so that you can do further operations e.g. joins without shuffles
   */
  val totalCountsCombine = wordCounts.combineByKey(
    (count: Int) => count,
    (currentSum: Int, newValue: Int) => currentSum + newValue,
    (partialSum1: Int, partialSum2: Int) => partialSum1 + partialSum2,
    numPartitions = 10
  )
  totalCountsCombine.collectAsMap()

  def main(args: Array[String]): Unit = {
    Thread.sleep(10000000)
  }

}

