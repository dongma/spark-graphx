package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.slf4j.LoggerFactory

/**
 * @author Sam Ma
 * @date 2020/07/06
 * 分布式共享变量 Distributed Shared Variables
 */
object SharedVariable {

  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("databricks spark application")
      .config("spark.master", "local")
      .getOrCreate()

    val collections = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
    val wordsRdd = spark.sparkContext.parallelize(collections, 2)

    // in scala中若在多个node上共享变量 可使用spark.sparkContext.broadcast(rdd)方法，取值方式为suppBroadCast.value
    val supplementalData = Map("Spark" -> 1000, "Definition" -> 200, "Big" -> -300, "Simple" -> 100)
    val suppBroadCast = spark.sparkContext.broadcast(supplementalData)
    // 在wordsRdd转换时使用 broadCast Variables, 当key字段存在时返回其value数值, 若其不存在则默认返0
    val wordPairs = wordsRdd.map(word => (word, suppBroadCast.value.getOrElse(word, 0)))
      .sortBy(wordPair => wordPair._2)
      .collect()
    // [(Big,-300), (The,0), (Definitive,0), (Guide,0), (:,0), (Data,0), (Processing,0), (Made,0), (Simple,100), (Spark,1000)]
    logger.info(s"shared variables map result is : {}", wordPairs)

    import spark.implicits._
    val flightsData = spark.read.parquet("/Users/madong/datahub-repository/spark-graphx/example-data/flight-data/parquet/2010-summary.parquet")
      .as[Flight]
    // 创建unnamed累加器accumulator 对所有从china出发或者终点为china的flight进行统计
    val namedAccumulator = new LongAccumulator
    val accChina = spark.sparkContext.longAccumulator("China")
    spark.sparkContext.register(namedAccumulator, "China")

    flightsData.foreach(flight_row => accChinaFunc(flight_row, namedAccumulator))
    logger.info(s"origin_country_name or dest_country_name is china accumulator values: {}", namedAccumulator.value)
  }

  /**
   * 判断飞行航班始发地中是否存在china,若存在则对数据进行累加
   *
   * @param flight_row
   */
  def accChinaFunc(flight_row: Flight, accChina: LongAccumulator) = {
    val destination = flight_row.DEST_COUNTRY_NAME
    val origin = flight_row.ORIGIN_COUNTRY_NAME
    if (destination == "China") {
      accChina.add(flight_row.count.toLong)
    }
    if (origin == "China") {
      accChina.add(flight_row.count.toLong)
    }
  }

}