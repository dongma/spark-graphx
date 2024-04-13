package org.apache.spark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 从不同的数据源读取数据，例如json、csv、jdbc以及Parquet
 *
 * @author Sam Ma
 * @date 2024/04/13
 */
object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val flightSchema = StructType(Array(
    StructField("ORIGIN_COUNTRY_NAME", StringType),
    StructField("DEST_COUNTRY_NAME", StringType),
    StructField("count", IntegerType)
  ))

  /*
   * Read a Df:
   * - format
   * - schema or inferSchema = true
   * - zero or more options
   */
  val flightDF = spark.read.format("json")
    .schema(flightSchema) // enforces a schema
    .option("mode", "failfast") // dropMalformed, permissive(default)
    .option("path", "example-data/flight-data/json/2010-summary.json")
    .load()
  flightDF.show(10)

  // alternative reading with options map
  val flightDFWithOptionMap = spark.read.format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "example-data/flight-data/json/2010-summary.json",
      "inferSchema" -> "true"
    )).load()

  /*
   Writing DFs
   - format
   - save mode = overwrite, append, ignore, errorIfExists
   - path
   - zero or more options
   */
  flightDF.write.format("json")
    .mode(SaveMode.Overwrite)
    .save("example-data/dfsave")

  // JSON flags，从json文件中读取数据
  spark.read.options(Map(
    "inferSchema" -> "true",
    "dateFormat" -> "YYYY-MM-dd", // couple with schema; if Spark fails parsing, it will put null
    "allowSingleQuotes" -> "true",
    "compress" -> "uncompressed" // bzip2, gzip, lz4, snappy, deflate
  )).json("example-data/rtjvm/cars.json")

  // CSV flags
  spark.read.schema(flightSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("example-data/flight-data/csv/2011-summary.csv")

  // Parquet
  flightDF.write.mode(SaveMode.Overwrite)
    .save("example-data/flight-data/flight.parquet")

  // Text file
  spark.read.text("README.md").show(10)

  // Reading from a remote DB
  val employeeDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()
  employeeDF.show(10)

  /**
   * Exercise: read the movies DF, then write it as
   *  - tab-separated values file
   *  - snappy Parquet
   *  - table "public.movies" in the Postgres DB
   */
  val movieDF = spark.read.json("example-data/rtjvm/movies.json")
  // TSV
  movieDF.write.format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .save("example-data/rtjvm/movies.csv")
  // Parquet
  movieDF.write.save("example-data/rtjvm/movies.parquet")
  // save to DB
  movieDF.write
    .format("jdbc")
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
      "user" -> "docker",
      "password" -> "docker",
      "dbtable" -> "public.movies"
    )).save()

}
