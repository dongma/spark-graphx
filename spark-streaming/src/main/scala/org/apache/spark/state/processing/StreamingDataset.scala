package org.apache.spark.state.processing

import org.apache.spark.Flight
import org.apache.spark.sql.SparkSession

/**
 * @author Sam Ma
 * @date 2020/07/21
 * Streaming Dataset API使用Dataset做类型检查来输出Stream
 */
object StreamingDataset {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Streaming Dataset")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", "/Users/madong/datahub-repository/spark-graphx/spark-warehouse")
      .getOrCreate()

    val dataSchema = spark.read
      .parquet("/Users/madong/datahub-repository/spark-graphx/example-data/flight-data/parquet/2010-summary.parquet")
      .schema

    import spark.implicits._
    val flightDf = spark.readStream.schema(dataSchema)
      .parquet("/Users/madong/datahub-repository/spark-graphx/example-data/flight-data/parquet/2010-summary.parquet")
    val flights = flightDf.as[Flight]

    val deviceCountQuery = flights.filter(flight_row => originIsDestination(flight_row))
      .groupByKey(row => row.DEST_COUNTRY_NAME).count()
      .writeStream.queryName("device_counts")
      .format("memory")
      .outputMode("complete")
      .start()

    deviceCountQuery.awaitTermination(2000)
    spark.sql("select * from device_counts").show(5)
  }

  /**
   * 判断同一条记录中航线的 ORIGIN_COUNTRY_NAME与DEST_COUNTRY_NAME是否相同
   *
   * @param flight_row
   * @return
   */
  def originIsDestination(flight_row: Flight): Boolean = {
    return flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
  }

}

