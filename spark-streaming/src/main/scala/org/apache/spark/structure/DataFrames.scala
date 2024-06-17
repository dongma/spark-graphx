package org.apache.spark.structure

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.concurrent.duration.DurationInt

/**
 * Streaming的应用，从socket、file中读取数据
 *
 * @author Sam Ma
 * @date 2024/05/21
 */
object DataFrames {

  val spark = SparkSession.builder().appName("Our first streams")
    .master("local[2]")
    .getOrCreate()

  def readFromSocket() = {
    // reading a DF
    val lines: DataFrame = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
    // transformation
    val shortLines: DataFrame = lines.filter(functions.length(col("value")) <= 5)

    // tell between a static vs a streaming DF
    println(shortLines.isStreaming)
    // consuming a DF
    val query = shortLines.writeStream.format("console")
      .outputMode("append")
      .start()
    // wait for the stream to finish
    query.awaitTermination()
  }

  val stockSchema = StructType(Array(
    StructField("company", StringType),
    StructField("date", DateType),
    StructField("value", DoubleType)
  ))

  def readFromFiles() = {
    val stocksDF = spark.readStream.format("csv")
      .option("header", "true").option("dateFormat", "MMM d yyyy")
      .schema(stockSchema)
      .load("example-data/rtjvm/stocks")

    stocksDF.writeStream.format("console")
      .outputMode("append")
      .start().awaitTermination()
  }

  /** stream流 micro batch不同的触发时机， ProcessingTime、Once、Continuous */
  def demoTriggers() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost").option("port", 12345).load()

    // write the lines DF at a certain trigger
    lines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(
        //        Trigger.ProcessingTime(2.seconds) // every 2 seconds run the query
        //        Trigger.Once()  // single batch, then terminate
        Trigger.Continuous(2.seconds) // experimental, every 2 seconds create a batch with whatever you have
      )
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readFromFiles()
  }

}
