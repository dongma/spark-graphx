package org.apache.spark.integrations

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util

/**
 * kafka与Low-level API DStream整合
 *
 * @author Sam Ma
 * @date 2024/06/19
 */
object KafkaDstream {

  val spark = SparkSession.builder()
    .appName("Spark Dstreams + Kafka")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  val kafkaParams: Map[String, Object] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.serializer" -> classOf[StringSerializer], // send data to kafka
    "value.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer], // receiving data from kafka
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> false.asInstanceOf[Object]
  )

  val kafkaTopic = "rockthejvm"

  def readFromKafka() = {
    val topics = Array(kafkaTopic)
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      /*
       Distributes the partitions evenly across the spark cluster.
       Alternatives:
       - PreferBrokers if the brokers and executors are in the same cluster
       - PreferFixed
       */
      LocationStrategies.PreferConsistent,
      /*
        Alternative:
        - SubscribePattern allows subscribing to topics matching a pattern
        - Assign - advanced; allows specifying offsets and partitions per topic
       */
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1"))
    )

    val processedStream = kafkaDStream.map(record => (record.key(), record.value()))
    processedStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def writeToKafka() = {
    val inputData = ssc.socketTextStream("localhost", 12345)
    // transform data
    val processedData = inputData.map(_.toUpperCase())
    processedData.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        // inside this lambda, the code is run by a single executor
        val kafkaHashMap = new util.HashMap[String, Object]()
        kafkaParams.foreach { pair =>
          kafkaHashMap.put(pair._1, pair._2)
        }

        // producer can insert records into kafka topics
        // available on this executor
        val producer = new KafkaProducer[String, String](kafkaHashMap)
        partition.foreach { value =>
          val message = new ProducerRecord[String, String](kafkaTopic, null, value)
          // feed the message into the kafka topic
          producer.send(message)
        }
        producer.close()
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeToKafka()
  }

}
