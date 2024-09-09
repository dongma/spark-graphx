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
  val totalCounts = wordCounts
    .groupByKey() // RDD of key = word, value = iterable of all previous values
    .mapValues(_.sum)

  // ^^ 6s for 2M laptop sales [maybe adjust numbers]
  // ^^ look at the shuffle write - it shuffles the entire data
  totalCounts.collectAsMap()

  // TODO: reduceByKey、foldByKey、aggregateByKey、combineByKey

}

