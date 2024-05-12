package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col}

/**
 * 处理Nulls数据
 *
 * @author Sam Ma
 * @date 2024/04/26
 */
object ManagingNulls extends App {

  val spark = SparkSession.builder().appName("Managing Nulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true")
    .json("example-data/rtjvm/movies.json")

  // select the first non-null value，coalesce函数主要用来处理空值
  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
  )

  // checking for nulls
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

  // nulls when ordering
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)

  // removing nulls
  moviesDF.select("Title", "IMDB_Rating").na.drop() // remove rows containing nulls

  // replacing nulls
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))
  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "Unknown"
  ))

  // complex operations
  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // same
    // returns null if the two values are EQUAL, else first value
    // Wilson|        7.0|                  null|  70.0|70.0|  null| 0.0|
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif",
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2" // if (first != null) second else third
  ).show()

}
