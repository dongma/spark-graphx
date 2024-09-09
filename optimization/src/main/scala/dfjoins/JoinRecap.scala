package dfjoins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Created by madong on 2024/8/23.
 */
object JoinRecap {

  val spark = SparkSession.builder().master("local[2]")
    .appName("Joins Recap")
    .getOrCreate()
  val sc = spark.sparkContext

  val rootPath = "/Users/madong/committer/spark-graphx"
  // 从rtjvm目录下读取json数据
  val guitarsDF = spark.read.option("inferSchema", "true")
    .json(s"${rootPath}/example-data/rtjvm/guitars")
  val guitarPlayersDF = spark.read.option("inferSchema", "true")
    .json(s"${rootPath}/example-data/rtjvm/guitarPlayers")
  val bandsDF = spark.read.option("inferSchema", "true")
    .json(s"${rootPath}/example-data/rtjvm/bands")

  // inner joins
  val joinCondition = guitarPlayersDF.col("band") === bandsDF.col("id")
  val guitaristBandsDF = guitarPlayersDF.join(bandsDF, joinCondition, "inner")

  // outer joins
  /* left outer = everything in inner join + all the rows in the left table, with nulls in the rows
   not passing the condition in the RIGHT table */
  guitarPlayersDF.join(bandsDF, joinCondition, "left_outer")

  // right outer,  everything in inner join + all the rows in the right table, with nulls in the rows
  //   not passing the condition in the LEFT table
  guitarPlayersDF.join(bandsDF, joinCondition, "right_outer")

  // outer join = everything in left_outer + right_outer
  guitarPlayersDF.join(bandsDF, joinCondition, "outer")

  // semi joins= everything in the left DF for which THERE IS a row in the right DF satisfying the condition
  // essentially a filter
  guitarPlayersDF.join(bandsDF, joinCondition, "left_semi")

  // anti joins = everything in the left DF for which THERE IS NOT a row in the right DF
  // satisfying the condition
  // also a filter
  guitarPlayersDF.join(bandsDF, joinCondition, "left_anti")

  /*
   Cross Join = everything in the left table with everything in the right table
   // dangerous: NRows(crossjoin) = NRows(left) * NRows(right)
   // careful with outer joins with non-unique keys
   */

  def main(args: Array[String]): Unit = {
    // RDD joins
    val colorsScores = Seq(
      ("blue", 1),
      ("red", 4),
      ("green", 5),
      ("yellow", 2),
      ("orange", 3),
      ("cyan", 0)
    )
    val colorsRDD: RDD[(String, Int)] = sc.parallelize(colorsScores)
    val text = "The sky is blue, but the orange pale sun turns from yellow to red"
    // standard technique for counting words with RDDs
    val words = text.split(" ").map(_.toLowerCase()).map((_, 1))
    // counting word occurrence
    val wordsRDD = sc.parallelize(words).reduceByKey(_+_)

    import spark.implicits._
    // (yellow, (1, 2)), (red, (1, 4))
    val scores: RDD[(String, (Int, Int))] = wordsRDD.join(colorsRDD)
    scores.toDF().show()
  }

}
