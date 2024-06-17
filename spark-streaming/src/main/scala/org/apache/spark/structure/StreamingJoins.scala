package org.apache.spark.structure

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}

/**
 * 使用数据流进行Join关联
 *
 * @author Sam Ma
 * @date 2024/06/02
 */
object StreamingJoins {

  val spark = SparkSession.builder()
    .appName("Streaming Joins")
    .master("local[2]")
    .config("spark.sql.crossJoin.enabled", "true")
    .getOrCreate()

  val guitarPlayers = spark.read.option("inferSchema", "true")
    .json("example-data/rtjvm/guitarPlayers")

  val guitars = spark.read.option("inferSchema", "true")
    .json("example-data/rtjvm/guitars")

  val bands = spark.read.option("inferSchema", "true")
    .json("example-data/rtjvm/bands")

  // joining static DFs
  val joinCondition = guitarPlayers.col("band") === bands.col("id")
  val guitaristsBands = guitarPlayers.join(bands, joinCondition, "inner")
  val bandsSchema = bands.schema

  def joinStreamWithStatic() = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // 从netcat中读取的是json串，例：{"id":1,"name":"AC/DC","hometown":"Sydney","year":1973}
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    // join happens PER BATCH
    val streamedBandsGuitaristsDF = streamedBandsDF
      .join(guitarPlayers, guitarPlayers.col("band") === streamedBandsDF.col("id"))
    /*
     restricted joins:
     - stream joining with static: RIGHT outer join/full outer join/right_semi not permitted
     - static joining with streaming: LEFT outer join/full/left_semi not permitted
     */
    streamedBandsGuitaristsDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  // since spark 2.3 we have stream joins
  def joinStreamWithStream() = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // 从netcat中读取的是json串，例：{"id":1,"name":"AC/DC","hometown":"Sydney","year":1973}
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    val streamedGuitaristsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load() // 从netcat中读取的是json串，例：{"id":1,"name":"AC/DC","hometown":"Sydney","year":1973}
      .select(from_json(col("value"), guitarPlayers.schema).as("guitarPlayer"))
      .selectExpr("guitarPlayer.id as id", "guitarPlayer.name as name",
        "guitarPlayer.guitars as guitars", "guitarPlayer.band as band")
    // join stream with stream
    val streamedJoin = streamedBandsDF.join(streamedGuitaristsDF,
      streamedGuitaristsDF.col("band") === streamedBandsDF.col("id"))

    /*
     - inner joins are supported
     - left/right outer joins ARE supported, bust MUST have watermarks
     - full outer joins are NOT supported
     */
    streamedJoin.writeStream
      .format("console")
      .outputMode("append") // only append supported for stream vs stream join
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    joinStreamWithStream()
  }

}
