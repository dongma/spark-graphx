package org.apache.spark

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Sam Ma
 * @date 2020/07/19
 * 用于从apache spark订阅topic并读取消息记录
 */
object Source {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Kafka Source Streaming").setMaster("local")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    /*
     * 配置kafka configuration相关信息，包括broker地址、key和value的序列化工具类
     */
    val kafkaConfigure = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // 指定连接kafka指定topic 并进行消费producer生产的最新的消息
    val topics = Array("streaming-data")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaConfigure)
    )
    stream.map(record => (record.key, record.value)).print()

    streamingContext.start()
    streamingContext.awaitTermination()

    // mention:下面的写法为spark streaming 2.2.0的写法（已经过时）实际运行时并不能将kafka的数据进行打印
    /*val streamingContext = SparkSession.builder().appName("Kafka Source Streaming")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", "/Users/madong/datahub-repository/spark-graphx/spark-warehouse")
      .getOrCreate()*/

    /*
     * 通过readStream从kafka获取指定topic下的消息，并将流式数据写入到临时表temp table中
     */
    /*val kafkaStream = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "192.168.0.101:9092")
      .option("subscribe", "streaming-data")
      .option("startingOffsets", "latest")
      .load()
    kafkaStream.printSchema()

    Thread.sleep(5000)
    val streamQuery = kafkaStream.writeStream.queryName("kafka_stream_table")
      .format("console")
      .outputMode("append")
      .start()
    streamQuery.awaitTermination()*/

  }

}
