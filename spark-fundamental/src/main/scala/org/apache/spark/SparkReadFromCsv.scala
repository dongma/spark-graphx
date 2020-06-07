package org.apache.spark

import org.apache.spark.sql.functions._
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession

/**
 * @author Sam Ma
 * @date 2020/06/07
 * apache spark加载csv文件到内存中，并使用Spark SQL进行view数据查询
 */
object SparkReadFromCsv {

  private[this] val logger = Logger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("databricks spark application")
      .config("spark.master", "local")
      .getOrCreate()

    val flightData2015 = spark.read.option("inferSchema", "true").option("header", "true")
      .csv("/Users/madong/datahub-repository/spark-graphx/example-data/flight-data/csv/2015-summary.csv")

    //  将dataFrame转换为view视图，用spark SQL进行查询
    flightData2015.createOrReplaceTempView("flight_data_2015")

    val sqlway = spark.sql(
      """
        select dest_country_name, count(1) from flight_data_2015 group by dest_country_name
        """)
    sqlway.explain()

    val dataFrameWay = flightData2015.groupBy("dest_country_name").count()
    dataFrameWay.explain()

    // 使用max()函数从flight_data_2015查找最大count field记录
    var maxCountValue = spark.sql("select max(count) from flight_data_2015").take(1)
    logger.info(s"max count value $maxCountValue in flight_data_2015 table")
    maxCountValue = flightData2015.select(max("count")).take(1)

    // 使用spark sql编写复杂的查询语句，按目的国家进行分组 取count总数最大的前5条记录
    val maxSql = spark.sql(
      """
        select dest_country_name, sum(count) as destination_total
        from flight_data_2015
        group by dest_country_name
        order by sum(count) desc
        limit 5
        """)
    maxSql.show()

    // 使用spark的DataFrame从临时表中查询相同的数据
    flightData2015.groupBy("dest_country_name")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort(desc("destination_total"))
      .limit(5)
      .show()

  }

  // spark-shell command: 从本地目录加载csv文件到apache spark中，通过take(3)从spark的DataFrame中取3行记录
  // val flightData2015 = spark.read.option("inferSchema", "true").option("header", "true").csv("/Users/madong/datahub-repository/spark-graphx/example-data/flight-data/csv/2015-summary.csv")
  // flightData2015.take(3)
  // flightData2015.sort("count").explain()
  // spark.conf.set("spark.sql.shuffle.partitions", "5") 设置shuffle分区的数量为5
  // scala> flightData2015.sort("count").take(2)
  // res3: Array[org.apache.spark.sql.Row] = Array([United States,Singapore,1], [Moldova,United States,1])
  // flightData2015.createOrReplaceTempView("fligh_data_2015")

}
