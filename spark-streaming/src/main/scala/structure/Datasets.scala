package structure

import common.{Car, carSchema}
import org.apache.spark.sql.functions.{avg, col, from_json}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

/**
 * Streaming Datasets
 *
 * @author Sam Ma
 * @date 2024/06/08
 */
object Datasets {

  val spark = SparkSession.builder()
    .appName("Streaming Datasets")
    .master("local[2]")
    .getOrCreate()

  // include encoders for DF -> DS transformations

  import spark.implicits._

  def readCars(): Dataset[Car] = {
    val carEncoder = Encoders.product[Car]
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // DF with single string column "value"
      .select(from_json(col("value"), carSchema).as("car")) // composite column (struct)
      .selectExpr("car.*") // DF with multiple columns
      .as[Car](carEncoder) // encoder can be passed implicitly with spark.implicits
  }

  def showCarName() = {
    val carsDS: Dataset[Car] = readCars()
    // transformation here
    val carNamesDF: DataFrame = carsDS.select(col("name")) // DF

    // collection transformations maintain type info
    val carNamesAlt: Dataset[String] = carsDS.map(_.Name)

    carNamesAlt.writeStream.format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  /**
   * Exercises:
   *
   * 1) Count how many POWERFUL cars we have in the DS (HP > 140)
   * 2) Average HP for the entire dataset, (use the complete output mode)
   * 3) Count the cars by origin
   */
  def exercise1(): Unit = {
    val carsDS = readCars()
    carsDS.filter(_.Horsepower.getOrElse(0L) > 140)
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def exercise2() = {
    val carsDS = readCars()
    carsDS.select(avg(col("HorsePower")))
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def exercise3() = {
    val carsDS = readCars()
    val carCountByOrigin = carsDS.groupBy(col("Origin")).count() // option 1
    val carCountByOriginAlt = carsDS.groupByKey(car => car.Origin).count() // option 2 with Dataset API
    carCountByOriginAlt
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    exercise3()
  }

}
