package org.apache.spark.integrations

import org.apache.spark.common.carSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, struct, to_json}

/**
 * Spark Streaming整合kafka
 *
 * @author Sam Ma
 */
object Kafka {

  val spark = SparkSession.builder()
    .appName("Integrating Kafka")
    .master("local[2]").getOrCreate()

  def readFromKafka() = {
    // https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "rockthejvm")
      .load()

    kafkaDF
      .select(col("topic"), expr("cast(value as string) as actualValue"))
      .writeStream
      .format("console")
      .outputMode("append")
      .start().awaitTermination()
  }

  def writeToKafka() = {
    val carsDF = spark.readStream.schema(carSchema)
      .json("example-data/rtjvm/cars")
    val carsKafkaDF = carsDF.selectExpr("upper(Name) as key, Name as value")
    carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints")  //  without checkpoints the writing will fail
      .start()
      .awaitTermination()
  }

  /**
   * Exercise: write the whole cars data structures to kafka as JSON.
   * use struct columns as the to_json function.
   */
  def writeCarsToKafka() = {
    val carsDF = spark.readStream
      .schema(carSchema)
      .json("example-data/rtjvm/cars")
    val carsJsonKafkaDF = carsDF.select(col("Name").as("key"),
      to_json(struct(col("Name"), col("Horsepower"), col("Origin")))
        .cast("String").as("value")
    )
    carsJsonKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints") //  without checkpoints the writing will fail
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeCarsToKafka()
  }

}
