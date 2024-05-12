package org.apache.spark

import org.apache.spark.sql.functions.{array_contains, avg, col}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

import java.sql.Date

/**
 * Datasets
 *
 * @author Sam Ma
 * @date 2024/04/26
 */
object Datasets extends App {

  val spark = SparkSession.builder().appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("example-data/rtjvm/numbers.csv")
  numbersDF.printSchema()

  // convert a DF to a Dataset
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  // dataset of a complex type
  // 1 - define your case class
  case class Car(
                Name: String,
                Miles_per_Gallon: Option[Double],
                Cylinders: Long,
                Displacement: Double,
                Horsepower: Long,
                Weight_in_lbs: Long,
                Acceleration: Double,
                Year: Date,
                Origin: String
                )

  // 2 - read the DF from the file
  def readDF(fileName: String) = spark.read
    .option("inferSchema", "true")
    .json(s"example-data/rtjvm/$fileName")

  // 3 - define an encoder (importing the implicits)
  import spark.implicits._
  val carsDF = readDF("cars.json")
  carsDF.filter(col("Miles_per_Gallon").isNull).show()
  // 4 - convert the DF to DS, Miles_per_Gallon字段存在null数据
  val carsDS = carsDF.na.drop().as[Car]

  // DS collection functions
  numbersDS.filter(_ < 100).show()

  // map、flatmap, fold, reduce, for comprehensions ...
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())
  carNamesDS.show()

  /**
   * Exercises:
   *
   * 1. Count how many cars we have
   * 2. Count how many POWERFUL cars we have (HP > 140)
   * 3. Average HP for the entire dataaset
   */
  // 1
  val carsCount = carsDS.count
  println(carsCount)
  // 2
  println(carsDS.filter(_.Horsepower > 140).count)
  // 3
  println(carsDS.map(_.Horsepower).reduce(_ + _) / carsCount)
  // 4 also use the DF functions
  carsDS.select(avg(col("Horsepower"))).show()

  // Joins
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name:String, hometown:String, year:Long)

  val guitarDS = readDF("guitars.json").as[Guitar]
  val guitarPlayerDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayerDS.joinWith(bandsDS,
    guitarPlayerDS.col("band") === bandsDS.col("id"), "inner")
  guitarPlayerBandsDS.show()

  /**
   * Exercise: join the guitarsDS and guitarPlayersDS, in an outer join
   * (hint: use array_contains)
   */
  guitarPlayerDS
    .joinWith(guitarDS, array_contains(guitarPlayerDS.col("guitars"), guitarDS.col("id")), "outer")
    .show()

  // Grouping DS, joins and groups are WIDE transformations, will involve SHUFFLE operations
  val carsGroupedByOrigin = carsDS.groupByKey(_.Origin)
    .count()
    .show()

}
