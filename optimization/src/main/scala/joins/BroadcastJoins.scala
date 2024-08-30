package joins

import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * 小表joins大表，SPARK参数可设置threshold
 *
 * @author Sam Ma
 * @date 2024/08/25
 */
object BroadcastJoins {

  val spark = SparkSession.builder()
    .appName("Broadcast Joins")
    .master("local").getOrCreate()
  val sc = spark.sparkContext

  val rows = sc.parallelize(List(
    Row(0, "zero"),
    Row(1, "first"),
    Row(2, "second"),
    Row(3, "third")
  ))

  val rowsSchema = StructType(Array(
    StructField("id", IntegerType),
    StructField("order", StringType)
  ))

  // small table
  val lookupTable: DataFrame = spark.createDataFrame(rows, rowsSchema)
  // large table
  val table = spark.range(1, 100000000)
  // join
  val joined = table.join(lookupTable, "id")
  joined.explain()
//  joined.show() - takes an ice age

  // a smarter join
  val joinedSmart = table.join(broadcast(lookupTable), "id")
  joinedSmart.explain()
  joinedSmart.show()

  // if the value is -1, deactivate auto-broadcast
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 30)
  // auto-broadcast detection
  val bigTable = spark.range(1, 100000000)
  val smallTable = spark.range(1, 10000) // size estimated by Spark - auto broadcast
  val joinedNumbers = smallTable.join(bigTable, "id")

  joinedNumbers.explain()

  def main(args: Array[String]): Unit = {
    Thread.sleep(100000000)
  }

}
