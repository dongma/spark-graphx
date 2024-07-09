package org.apache.spark.advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * 事件ProcessTime时间窗口
 *
 * @author Sam Ma
 * @date 2024/07/03
 */
object ProcessTimeWindow {

  val spark = SparkSession.builder()
    .appName("Processing Time Windows")
    .master("local[2]")
    .getOrCreate()

  def aggregateByProcessingTime() = {
    val linesCharCountByWindowDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .select(col("value"), current_timestamp().as("processingTime"))
      .groupBy(window(col("processingTime"), "10 seconds")
        .as("window"))
      // counting characters every 10 seconds by processing time
      .agg(sum(length(col("value"))).as("charCount"))
      .select(
        col("window").getField("start").as("start"),
        col("window").getField("end").as("end"),
        col("charCount")
      )

    linesCharCountByWindowDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    aggregateByProcessingTime()
  }

}
