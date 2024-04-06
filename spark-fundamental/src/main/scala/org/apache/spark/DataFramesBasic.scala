package org.apache.spark

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @author Sam Ma
 * @date 2020/06/07
 * apache spark加载csv文件到内存中，并使用Spark SQL进行view数据查询
 */
object DataFramesBasic {

  private[this] val logger = Logger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("databricks spark application")
      .config("spark.master", "local")
      .getOrCreate()

    val flightData2015 = spark.read.option("inferSchema", "true").option("header", "true")
      .csv("/Users/madong/datahub-repository/spark-graphx/example-data/flight-data/csv/2015-summary.csv")
    flightData2015.show() // showing a DF
    flightData2015.printSchema()

    flightData2015.take(10).foreach(println)
    val longType = LongType // spark types

    // schema
    val citySchema = StructType(Array(
      StructField("DEST_COUNTRY_NAME", StringType),
      StructField("ORIGIN_COUNTRY_NAME", StringType),
      StructField("count", IntegerType)
    ))
    val cityDFSchema = flightData2015.schema
    println(cityDFSchema)

    // create rows by hand
    val myRow = Row("China", "Beijing", 21)
    // create DF from tuples
    val cities = Seq(
      ("China", "Nanjing", 14),
      ("China", "ChengDu", 16)
    )
    val manualCityDf = spark.createDataFrame(cities)  // schema auto-inferred
    // note: DFs have schemas, rows do not

    // create DFs with implicits
    import spark.implicits._
    val manualCitiesDfWithImplicits = cities.toDF("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME", "count")
    manualCityDf.printSchema()
    manualCitiesDfWithImplicits.printSchema()

    /**
     * Exercise:
     * 1) Create a manual DF description smartphones
     *  - contains: make、model、screen description and camera megapixels
     * 2) Read another file from the data/ folder, eg. movie.json
     *  - print its schema
     *  - count the number of rows, call count()
     */
    val smartphones = Seq(
      ("Samsung", "Galaxy S10", "Android", 12),
      ("Apple", "Apple X", "iOS", 13),
      ("Nikia", "3310", "THE BEST", 0)
    )
    val smartphonesDF = smartphones.toDF("Make", "Model", "Platform", "CameraMegapixels")
    smartphonesDF.show()

    // 2
    val flightDF = spark.read.format("json").option("inferSchema", "true")
      .load("/Users/madong/datahub-repository/spark-graphx/example-data/flight-data/json/2010-summary.json")
    flightDF.printSchema()
    println(s"The flightDF has ${flightDF.count()} rows")
  }

}
