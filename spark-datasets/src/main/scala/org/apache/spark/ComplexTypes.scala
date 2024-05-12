package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * complex types
 *
 * @author Sam Ma
 * @date 2024/04/21
 */
object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true")
    .json("example-data/rtjvm/movies.json")

  // Dates
  val moviesWithReleaseDates = moviesDF.select(col("Title"),
    to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release")) // conversion

  moviesWithReleaseDates
    .withColumn("Today", current_date()) // today
    .withColumn("Right_Now", current_timestamp()) // this second
    .withColumn("Movie_Age",
      datediff(col("Today"), col("Actual_Release")) / 365) // date_add date_sub
  moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull).show()

  /**
   * Exercise
   * 1、How do we deal with multiple date formats?
   * 2、Read the stocks DF and parse the dates
   */
  // 1 - parse the DF multiple times, then union the small DFs

  // 2-
  val stocksDF = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("example-data/rtjvm/stocks.csv")

  val stocksDFWithDates = stocksDF
    .withColumn("actual_date", to_date(col("date"), "MMM dd yyyy"))
//  stocksDFWithDates.show()

  // Structures
  // 1- with col operators, struct先生成了结构体类型
  moviesDF
    .select(col("Title"),
      struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"),
      col("Profit").getField("US_Gross").as("US_Profit"))
//    .show()

  // 2- with expression with strings
  moviesDF
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")
    .show()

  // Arrays，在split的正则中，分割符为" "或","逗号（这两种）
  val moviesWithWords = moviesDF.select(
    col("Title"),
    split(col("Title"), " |,").as("Title_Words") // Array of strings
  )
  moviesWithWords.show()

  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
  ).show()

}
