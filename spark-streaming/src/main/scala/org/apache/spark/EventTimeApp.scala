package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author Sam Ma
 * @date 2020/07/22
 * EventTime Processing and Stateful Processing in DStream API
 */
object EventTimeApp {

  private[this] val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("EventTime app")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", "/Users/madong/datahub-repository/spark-graphx/spark-warehouse")
      .config("spark.sql.streaming.schemaInference", "true")
      // 设置数据处理分区数partitions，避免在local master产生过多分区
      .config("spark.sql.shuffle.partitions", 5)
      .getOrCreate()

    val static = spark.read.json("/Users/madong/datahub-repository/spark-graphx/example-data/activity-data")
    val streaming = spark.readStream.schema(static.schema)
      .option("maxFilesPerTrigger", 10)
      .json("/Users/madong/datahub-repository/spark-graphx/example-data/activity-data")
    streaming.printSchema()

    // 将json数据中的Creation_time字段转换成为timestamp类型，可根据window()获取streaming记录
    val withEventTime = streaming.selectExpr(
      "*",
      "cast(cast(Creation_time as double)/1000000000 as timestamp) as event_time")

    import org.apache.spark.sql.functions.{window, col}
    val eventPerWindowQuery = withEventTime.groupBy(window(col("event_time"), "10 minutes")).count()
      .writeStream
      .queryName("events_per_window")
      .format("memory")
      .outputMode("complete")
      .start()

    /* root
     |-- window: struct (nullable = false)
     |    |-- start: timestamp (nullable = true)
     |    |-- end: timestamp (nullable = true)
     |-- count: long (nullable = false)
     */
    spark.sql("select * from events_per_window").printSchema()

    /*+--------------------+-----+
      |              window|count|
      +--------------------+-----+
      |[2015-02-23 18:40...| 1104|
      |[2015-02-24 19:50...| 1849|
      +--------------------+-----+
     */
    eventPerWindowQuery.awaitTermination(timeoutMs = 5000)
    spark.sql("select * from events_per_window").show(2)

  }

}
