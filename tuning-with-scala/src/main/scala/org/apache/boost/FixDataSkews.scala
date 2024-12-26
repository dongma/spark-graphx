package org.apache.boost

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.util.{Generator, Guitar, GuitarSale}

/**
 * 修复spark Job数据倾斜的问题
 *
 * @author Sam Ma
 * @date 2024/12/25
 */
object FixDataSkews {

  val spark = SparkSession.builder().appName("Fixing data skews")
    .master("local[*]")
    .getOrCreate()

  // deactivate broadcast join
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
  import spark.implicits._

  val guitars: Dataset[Guitar] = Seq.fill(40000)(Generator.randomGuitar()).toDS
  val guitarSales: Dataset[GuitarSale] = Seq.fill(200000)(Generator.randomGuitarSale()).toDS
  /*
   A guitar is similar to a GuitarSale if, 耗费时间大约1.6min
   - same make and model
   - abs(guitar.soundScore - guitarSale.soundScore) <= 0.1

   Problems:
   - for every Guitar, avg(sale prices of ALL SIMILAR GuitarSales)
   - Gibson L-00, config "sadfhia", sound 4.3
   compute avg(sale prices of ALL GuitarSales of Gibson L-00 with sound quality between 4.2 and 4.4)
   */
  def naiveSolution() = {
    val joined = guitars.join(guitarSales, Seq("make", "model"))
      .where(abs(guitarSales("soundScore") - guitars("soundScore")) <= 0.1)
      .groupBy("configurationId")
      .agg(avg("salePrice").as("averagePrice"))

    joined.explain()
    joined.count()
  }

  /*
    加入salt字段，使数据的分布相对来说更均匀一些，耗费时间在26s
   */
  def noSkewSolution() = {
    // salting interval 0 -99, multiplying the guitars DS * 100
    val explodedGuitars = guitars.withColumn("salt", explode(lit((0 to 99).toArray)))
    val saltedGuitarSales = guitarSales.withColumn("salt", monotonically_increasing_id() % 100)

    val nonSkewedJoin = explodedGuitars.join(saltedGuitarSales, Seq("make", "model", "salt"))
      .where(abs(saltedGuitarSales("soundScore") - explodedGuitars("soundScore")) <= 0.1)
      .groupBy("configurationId")
      .agg(avg("salePrice").as("averagePrice"))

    nonSkewedJoin.explain()
    nonSkewedJoin.count()
  }

  def main(args: Array[String]): Unit = {
    noSkewSolution()
    Thread.sleep(100000)
  }

}
