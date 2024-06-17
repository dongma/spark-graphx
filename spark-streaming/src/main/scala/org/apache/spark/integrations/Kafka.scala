package org.apache.spark.integrations

import org.apache.spark.sql.SparkSession

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

    kafkaDF.writeStream.format("console")
      .outputMode("append")
      .start().awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readFromKafka()
  }

}
