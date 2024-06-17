package org.apache.spark.structure

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * 数据流进行Aggregation聚合
 *
 * @author Sam Ma
 * @date 2024/05/28
 */
object Aggregations {

  val spark = SparkSession.builder()
    .appName("Streaming Aggregations")
    .master("local[2]")
    .getOrCreate()

  def streamingCount() = {
    val lines: DataFrame = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", 12345).load()

    val lineCount: DataFrame = lines.selectExpr("count(*) as lineCount")
    // aggregations with distinct are not supported
    // otherwise SPARK WILL NEED TO KEEP TRACK OF EVERYTHING

    lineCount.writeStream.format("console")
      .outputMode("complete") // append and update not supported on aggregations without watermark
      .start()
      .awaitTermination()
  }

  def numericalAggregations(aggFunction: Column => Column): Unit = {
    val lines = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", "12345")
      .load()

    // aggregate here
    val numbers = lines.select(col("value").cast("integer").as("number"))
    val sumDF = numbers.select(aggFunction(col("number")).as("agg_so_far"))

    sumDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def groupNames(): Unit = {
    val lines = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", "12345")
      .load()

    // counting each occurrence of the "name" value
    val names = lines.select(col("value").as("name"))
      .groupBy(col("name")) // RelationalGroupedDataset
      .count()
    names.writeStream.format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    groupNames()
  }

}
