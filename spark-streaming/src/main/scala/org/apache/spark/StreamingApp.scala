package org.apache.spark

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession

/**
 * @author Sam Ma
 * @date 2020/06/07
 * spark工具集概览，./bin/spark-submit通过jar包提交任务 Stream流式数据计算
 */
object StreamingApp {

  private[this] val logger = Logger(this.getClass)

  def main(args: Array[String]): Unit = {
    // 通过spark-submit使用jar包向spark提交任务，并可以指定partitions分区数量
    // ./bin/spark-submit --class org.apache.spark.examples.SparkPi --master local examples/jars/spark-examples_2.11-2.4.5.jar 10

    val spark = SparkSession.builder().appName("databricks spark application")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._
    val flightDataFrame = spark.read.parquet("/Users/madong/datahub-repository/spark-graphx/example-data/flight-data/parquet/2010-summary.parquet")
    val flights = flightDataFrame.as[Flight]
    // 在转换后的所有结果中filter出发地不为Canada的5条记录，对于类型将parquet转换为Flight 需要导入spark的隐式转换
    val otherCountryFlights = flights.filter(flight_row => flight_row.DEST_COUNTRY_NAME != "Canada")
      .map(flight_row => flight_row)
      .take(5)
    logger.info(s"canada isn't the origin_country 5 random records: $otherCountryFlights")

    // 从DataFrame中取5条记录，对其进行过滤并转换成为Flight类型
    val flightObjects = flights.take(5)
      .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
      .map(fr => Flight(fr.DEST_COUNTRY_NAME, fr.ORIGIN_COUNTRY_NAME, fr.count + 5))
    logger.info(s"get mapped Flight object: $flightObjects")

    // structured streaming, apache spark与流式系统对接生成DataFrame并进行流式数据计算
    val staticDataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/Users/madong/datahub-repository/spark-graphx/example-data/retail-data/by-day/*.csv")
    staticDataFrame.createGlobalTempView("retail_data")
    val staticSchema = staticDataFrame.schema

    import org.apache.spark.sql.functions.{col, window}
    // 默认spark上executors数量应该为200，但目前开发环境机器硬件性能并不高，可通过conf设置partitions数量为5
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    staticDataFrame.selectExpr(
      "CustomerId",
      "(UnitPrice * Quantity) as total_cost",
      "InvoiceDate")
      .groupBy(
        col("CustomerId"), window(col("InvoiceDate"), "1 day"))
      .sum("total_cost")
      .show(5)

    // spark通过stream方式读入每天销售的流式数据（按天进行读取，一次最多只能处理1个csv文件）
    val streamingDataFrame = spark.readStream.schema(staticSchema)
      .option("maxFilesPerTrigger", 1)
      .format("csv")
      .option("header", "true")
      .load("/Users/madong/datahub-repository/spark-graphx/example-data/retail-data/by-day/*.csv")
    logger.info("whether our dataframe is streaming: " + streamingDataFrame.isStreaming)

    // spark stream针对数据流的操作并不会立即执行，需调用一个stream流式操作开始数据流的执行
    val purchaseByCustomerPerHour = streamingDataFrame
      .selectExpr("CustomerId",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")
      .groupBy($"CustomerId", window($"InvoiceDate", "1 day"))
      .sum("total_cost")

    // 将spark计算的结果写入到内存的临时表中，表名称为customer_purchases 所有的count必须在同一张表中 (目前未查出数据)
    purchaseByCustomerPerHour.writeStream
      .format("memory") // store in-memory table
      .queryName("customer_purchases")
      .outputMode("complete")
      .start()

    spark.sql(
      """
        select * from customer_purchases order by `sum(total_cost)` desc
        """).show(5)
  }

  case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)

}
