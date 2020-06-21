package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory


/**
 * @author Sam Ma
 * @date 2020/06/21
 * Apache spark datasets数据集对数据 Transformation Filtering Mapping and Joins etc
 */
object DatasetsInSpark {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("databricks spark application")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._
    val flightDf = spark.read
      .parquet("/Users/madong/datahub-repository/spark-graphx/example-data/flight-data/parquet/2010-summary.parquet")
    val flights = flightDf.as[Flight]
    flightDf.show(2)
    // 当实际去access其中任意一个case class时，并不需要任何一个type 简单的指定case类中属性的名称就可以
    val firstCountryName = flights.first.DEST_COUNTRY_NAME
    logger.info(s"flights.first.DEST_COUNTRY_NAME value in flightDf: $firstCountryName")

    val filterRow = flights.filter(flight_row => originIsDestination(flight_row)).first()
    logger.info(s"first row which the origin_country_name equals dest_country_name values: $filterRow")
    val mapValue = flights.map(flight => flight.DEST_COUNTRY_NAME)
    val localDestinations = mapValue.take(5)
    logger.info(s"mapping value of 5 destinations: $localDestinations")

    val flightMetas = spark.range(500).map(x => (x, scala.util.Random.nextLong))
      .withColumnRenamed("_1", "count").withColumnRenamed("_2", "randomData")
      .as[FlightWithMetadata]
    val flightsJoinWith = flights.joinWith(flightMetas, flights.col("count") === flightMetas.col("count"))
    val takeWithValue = flightsJoinWith.take(2)
    logger.info(s"apache spark dataset with take 2 value: $takeWithValue")
    // 对dataset数据集按照DEST_COUNTRY_NAME字段进行分组，也可使用groupByKey(x => x.DEST_COUNTRY_NAME).count()分组
    val count = flights.groupBy("DEST_COUNTRY_NAME").count()
    logger.info(s"group flights dataset with field DEST_COUNTRY_NAME, count value: $count")

    // 按照DEST_COUNTRY_NAME字段对flights记录分组，并进行flatMapGroups转换(从中移除count <5 Flight)
    flights.groupByKey(x => x.DEST_COUNTRY_NAME).flatMapGroups(grpSum).show(5)
    // 在dataset数据集上执行map-reduce操作，按DEST_COUNTRY_NAME对flights数据进行分组，然后将分组数据的count进行相加
    flights.groupByKey(x => x.DEST_COUNTRY_NAME).reduceGroups((left, right) => sum2(left, right)).take(5)
  }

  /**
   * 在DataSet上定义filter函数，找DEST_COUNTRY_NAME与ORIGIN_COUNTRY_NAME相同的记录
   *
   * @param flight_row
   * @return
   */
  def originIsDestination(flight_row: Flight): Boolean = {
    flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
  }

  /**
   * 定义group函数将分组后Flight.count()数量小于5的记录移除
   *
   * @param countryName
   * @param values
   * @return
   */
  def grpSum(countryName: String, values: Iterator[Flight]) = {
    values.dropWhile(_.count < 5).map(x => (countryName, x))
  }

  /**
   * 对FLight对象进行合并，将Flight.count值进行相加
   *
   * @param left
   * @param right
   * @return
   */
  def sum2(left: Flight, right: Flight) = {
    Flight(left.DEST_COUNTRY_NAME, null, left.count + right.count)
  }

}
