package joins

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Created by madong on 2024/8/29.
 */
object PrePartitioning {

  val spark = SparkSession.builder().appName("Pre-Partitioning")
    .master("local[2]")
    .getOrCreate()
  import spark.implicits._

  // deactivate broadcast joins
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  /*
    add columns(initialTable, 3) => dataframe with column: "id", "newCol1", "newCol2", "newCol3"
   */
  def addColumns[T](df: Dataset[T], n: Int): DataFrame = {
    val newColumns = (1 to n).map(i => s"id * $i as newCol$i")
    df.selectExpr(("id" +: newColumns): _*)
  }

  // don't touch this
  val initialTable = spark.range(1 , 10000000).repartition(10)
  val narrowTable = spark.range(1 , 5000000).repartition(7)

  def main(args: Array[String]): Unit = {

  }

}
