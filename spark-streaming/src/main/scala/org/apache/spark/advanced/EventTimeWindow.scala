package org.apache.spark.advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * spark streaming事件窗口
 *
 * @author Sam Ma
 * @date 2024/07/01
 */
object EventTimeWindow {

  val spark = SparkSession.builder()
    .appName("Event Time Window")
    .master("local[2]")
    .getOrCreate()

  val onlinePurchaseSchema = StructType(Array(
    StructField("id", StringType),
    StructField("time", TimestampType),
    StructField("item", StringType),
    StructField("quantity", IntegerType)
  ))

  def readPurchasesFromSocket() = spark.readStream
    .format("socket")
    .option("host", "127.0.0.1")
    .option("port", 12346)
    .load()
    .select(from_json(col("value"), onlinePurchaseSchema).as("purchase"))
    .selectExpr("purchase.*")

  def readPurchasesFromFile() = spark.readStream
    .schema(onlinePurchaseSchema)
    .json("example-data/rtjvm/purchases")

  def aggregatePurchasesBySlidingWindow() = {
    val purchasesDF = readPurchasesFromSocket()

    val windowByDay = purchasesDF
      // struct column: has fields {start, end}
      .groupBy(window(col("time"), "1 day", "1 hour").as("time"))
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("quantity")
      )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
   * Exercises:
   * 1) Show the best selling product of every day, +quantity sold.
   * 2) Show the best selling product of every 24 hours, updated every hour.
   */
  def bestSellingProductPerDay() = {
    val purchasesDF = readPurchasesFromFile()
    val bestSelling = purchasesDF.groupBy(
      col("item"), window(col("time"), "1 day").as("day"))
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("day").getField("start").as("start"),
        col("day").getField("end").as("end"),
        col("item"),
        col("totalQuantity")
      )
      .orderBy(col("day"), col("totalQuantity").desc)

    bestSelling.writeStream
      .format("console")
      .outputMode("complete")
      .start().awaitTermination()
  }

  def bestSellingProductEvery24h() = {
    val purchasesDF = readPurchasesFromFile()
    val bestSelling = purchasesDF.groupBy(
      col("item"), window(col("time"), "1 day", "1 hour").as("day"))
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("day").getField("start").as("start"),
        col("day").getField("end").as("end"),
        col("item"),
        col("totalQuantity")
      )
      .orderBy(col("day"), col("totalQuantity").desc)

    bestSelling.writeStream
      .format("console")
      .outputMode("complete")
      .start().awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    bestSellingProductEvery24h()
  }

}
