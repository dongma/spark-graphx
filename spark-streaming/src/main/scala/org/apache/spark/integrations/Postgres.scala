package org.apache.spark.integrations

import org.apache.spark.common.{Car, carSchema}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Postgres与Low-level API DStream整合
 *
 * @author Sam Ma
 * @date 2024/06/19
 */
object Postgres {

  val spark = SparkSession.builder()
    .appName("Integrating JDBC")
    .master("local[2]")
    .getOrCreate()

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def writeStreamToPostgres() = {
    val carsDF = spark.readStream
      .schema(carSchema)
      .json("example-data/rtjvm/cars")

    import spark.implicits._
    val carsDS = carsDF.as[Car]

    carsDS.writeStream
      .foreachBatch { (batch: Dataset[Car], _: Long) =>
        // each executor can control the batch
        // batch is a STATIC Dataset/DataFrame
        batch.write
          .format("jdbc")
          .options(Map(
            "driver" -> driver,
            "url" -> url,
            "user" -> user,
            "password" -> password,
            "dbtable" -> "public.cars"
          ))
          .save()
      }
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeStreamToPostgres()
  }

}
