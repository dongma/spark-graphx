package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author Sam Ma
 * @date 2020/07/19
 * 使用spark streaming进行流式数据处理（线上数据实时计算）
 */
object StreamingApp {

  private[this] val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    /*
     * start up the sparkSession along with explicitly setting a given config
     */
    val spark = SparkSession.builder().appName("Spark Streaming")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", "/Users/madong/datahub-repository/spark-graphx/spark-warehouse")
      .config("spark.sql.streaming.schemaInference", "true")
      // 设置数据处理分区数partitions，避免在local master产生过多分区
      .config("spark.sql.shuffle.partitions", 5)
      .getOrCreate()

    val static = spark.read.json("/Users/madong/datahub-repository/spark-graphx/example-data/activity-data")
    val dataSchema = static.schema
    /*
     * 在spark config中显式指定 spark.sql.streaming.schemaInference配置，可通过streaming根据流读取数据
     */
    val streaming = spark.readStream.schema(dataSchema).option("maxFilesPerTrigger", 1)
      .json("/Users/madong/datahub-repository/spark-graphx/example-data/activity-data")
    val activityCounts = streaming.groupBy("gt").count()
    logger.info(s"streaming activity-data groupBy gt field, counts: $activityCounts")

    /*
     * 将streaming分组后的数据activityCounts写入到内存表memory中
     */
    val activityQuery = activityCounts.writeStream.queryName("activity_counts")
      .format("memory").outputMode("complete")
      .start()
    // 使用Query.awaitTermination()会使得driver process一直处于active的状态，默认情况下stream会在background运行
    activityQuery.awaitTermination(timeoutMs = 1500)
    logger.info(s"active streaming list ${spark.streams.active}")

    for(index <- 1 to 3) {
      spark.sql("select * from activity_counts").show(5)
      Thread.sleep(1000)
    }

    /*
     * 从临时生成的中间表activity_counts中筛选gt字段值为'%stairs%'的数据，并将匹配到的write到simple_transform中
     */
    import org.apache.spark.sql.functions._
    val simpleTransform = streaming.withColumn("stairs", expr("gt like '%stairs%'"))
      .where("stairs")
      .where("gt is not null")
      .select("gt", "model", "arrival_time", "creation_time")
      .writeStream
      .queryName("simple_transform")
      .format("memory")
      .outputMode("append")
      .start()

    simpleTransform.awaitTermination(timeoutMs = 2000)
    spark.sql("select * from simple_transform").show(5)

    /*
     * 使用spark streaming对流式进行聚合Aggregations、连接join spark中的cube函数
     */
    val deviceModelState = streaming.cube("gt", "model")
      .avg()
      .drop("avg(Arrival_time)")
      .drop("avg(Creation_time)")
      .drop("avg(Index)")
      .writeStream.queryName("device_counts").format("memory").outputMode("complete")
      .start()
    deviceModelState.awaitTermination(timeoutMs = 2000)
    spark.sql("select * from device_counts").show(5)

    // 使用join()对两个streaming内存数据表进行连接，字段列表为Seq("gt", "model")这两个字段
    val historicalAgg = static.groupBy("gt", "model").avg()
    val joinDeviceState = streaming.drop("Arrival_Time", "Creation_Time", "Index")
      .cube("gt", "model")
      .avg()
      .join(historicalAgg, Seq("gt", "model"))
      .writeStream.queryName("device_counts_join").format("memory")
      .outputMode("complete")
      .start()
    joinDeviceState.awaitTermination(timeoutMs = 5000)
    spark.sql("select * from device_counts_join").show(10)

    /*
     * 将dataframe中加载的数据集写入到apache kafka中，写入的topic名称为streaming-data
     * docker bash : docker exec -it kafka bash
     * cd /opt/kafka_2.11-2.0.0/bin/
     * ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic streaming-data --from-beginning
     */
    val kafkaSink = streaming.select(to_json(struct("Device", "gt", "x", "y", "z", "Arrival_Time", "Creation_Time")).alias("value"))
      .writeStream.format("kafka")
      .option("checkpointLocation", "/Users/madong/datahub-repository/spark-graphx/spark-warehouse/dir")
      .option("kafka.bootstrap.servers", "192.168.0.101:9092")
      .option("topic", "streaming-data")
      .start()
    kafkaSink.awaitTermination()

    /*
     * when data is output(Triggers), time trigger将会等待多个指定的duration来输出数据(未验证)
     */
    activityCounts.writeStream.trigger(Trigger.ProcessingTime("100 seconds"))
      .format("console").outputMode("complete").start()

    // 可以通过Trigger.Once()频率trigger触发执行给定streaming job一次
    activityCounts.writeStream.trigger(Trigger.Once())
      .format("console").outputMode("complete").start()
  }

}
