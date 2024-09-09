package rddjoins

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Created by madong on 2024/9/04.
 */
object CoGrouping {

  val spark = SparkSession.builder().appName("Co Grouping")
    .master("local[*]")
    .getOrCreate()
  val sc = spark.sparkContext
  val rootFolder = "example-data/rtjvm/exam_data"

  /*
   Take all the students attempts:
   - if a student passed (at least one attempt > 9.0), send them an email "PASSED"
   - else send them an email with "FAILED"
   */
  def readIds() = sc.textFile(s"$rootFolder/examIds.txt")
    .map { line =>
      val tokens = line.split(" ")
      (tokens(0).toLong, tokens(1))
    }
    .partitionBy(new HashPartitioner(10))

  def readExamScores() = sc.textFile(s"$rootFolder/examScores.txt")
    .map { line =>
      val tokens = line.split(" ")
      (tokens(0).toLong, tokens(1).toDouble)
    }

  def readExamEmails() = sc.textFile(s"$rootFolder/examEmails.txt")
    .map {line =>
      val tokens = line.split(" ")
      (tokens(0).toLong, tokens(1))
    }

  def plainJoin() = {
    val scores = readExamScores().reduceByKey(Math.max)
    val candidates = readIds()
    val emails = readExamEmails()

    val results = candidates
      .join(scores) // RDD[(Long, (String, Double))]
      .join(emails) // RDD[(Long, ((String, Double), String))]
      .mapValues {
        case ((_, maxAttempt), email) =>
          if (maxAttempt >= 9.0) (email, "PASSED")
          else (email, "FAILED")
      }
    results.count()
  }

  def coGroupedJoin() = {
    val scores = readExamScores().reduceByKey(Math.max)
    val candidates = readIds()
    val emails = readExamEmails()

    // three RDDs share the same partitioner
    val result: RDD[(Long, Option[(String, String)])] = candidates.cogroup(scores, emails)
      .mapValues {
        case (nameIterable, maxAttemptIterable, emailIterable) =>
          val name = nameIterable.headOption
          val maxScore = maxAttemptIterable.headOption
          val email = emailIterable.headOption

          for {
            e <- email
            s <- maxScore
          } yield (e, if (s >= 9.0) "PASSED" else "FAILED")
      }
    result.count()
  }

  def main(args: Array[String]): Unit = {
    plainJoin()
    coGroupedJoin()
    Thread.sleep(1000000)
  }

}
