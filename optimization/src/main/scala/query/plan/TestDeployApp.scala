package query.plan

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Created by madong on 2024/8/17.
 */
object TestDeployApp {

  // TestDeployApp inputFile outputFile
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Need input file and output file")
      System.exit(1)
    }

    val spark = SparkSession.builder().appName("Test Deploy App")
      .getOrCreate()
    import spark.implicits._

    val moviesDF = spark.read.option("inferSchema", "true")
      .json(args(0))
    moviesDF.show(10)

    val goodComediesDF = moviesDF.selectExpr(
      "Title",
      "IMDB_Rating as Rating",
      "Release_Date as Release"
    )
      .where(($"Major_Genre" === "Comedy") and ($"IMDB_Rating" > 6.5))
      .orderBy($"Rating".desc_nulls_last)
    goodComediesDF.show(10)

    goodComediesDF.write.mode(SaveMode.Overwrite)
      .format("json")
      .save(args(1))
  }

}
