package joins

import org.apache.spark.sql.SparkSession

/**
 * Created by madong on 2024/9/01.
 */
object Bucketing {

  val spark = SparkSession.builder().appName("bucketing")
    .master("local")
    .getOrCreate()

  import spark.implicits._
  // deactivate broadcasting
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
  val large = spark.range(1000000).selectExpr("id * 5 as id").repartition(10)
  val small = spark.range(10000).selectExpr("id * 3 as id").repartition(3)

  val joined = large.join(small, "id")
  joined.explain()

  // bucketing
  large.write
    .bucketBy(4, "id")
    .sortBy("id")
    .mode("overwrite")
    .saveAsTable("bucketed_large")

  small.write
    .bucketBy(4, "id")
    .sortBy("id")
    .mode("overwrite")
    .saveAsTable("bucketed_small")  // bucketing and saving almost as expensive as a regular shuffle

  val bucketedLarge = spark.table("bucketed_large")
  val bucketedSmall = spark.table("bucketed_small")
  val bucketedJoin = bucketedLarge.join(bucketedSmall, "id")
  bucketedJoin.explain()
  /*
  == Physical Plan ==
  AdaptiveSparkPlan isFinalPlan=false
  +- Project [id#0L]
     +- SortMergeJoin [id#0L], [id#4L], Inner
        :- Sort [id#0L ASC NULLS FIRST], false, 0
        :  +- Exchange hashpartitioning(id#0L, 200), ENSURE_REQUIREMENTS, [plan_id=343]
        :     +- Exchange RoundRobinPartitioning(10), REPARTITION_BY_NUM, [plan_id=336]
        :        +- Range (1, 10000000, step=1, splits=2)
        +- Sort [id#4L ASC NULLS FIRST], false, 0
           +- Exchange hashpartitioning(id#4L, 200), ENSURE_REQUIREMENTS, [plan_id=344]
              +- Exchange RoundRobinPartitioning(10), REPARTITION_BY_NUM, [plan_id=338]
                 +- Range (1, 5000000, step=1, splits=2)
   */

  // bucketing for groups
  val flightsDF = spark.read.option("inferSchema", "true")
    .json("example-data/rtjvm/flights/flights.json")
    .repartition(2)

  val mostDelayed = flightsDF
    .filter("origin = 'DEN' and arrdelay > 1")
    .groupBy("origin", "dest", "carrier")
    .avg("arrdelay")
    .orderBy($"avg(arrdelay)".desc_nulls_last)
  mostDelayed.explain()

  flightsDF.write
    .partitionBy("origin")
    .bucketBy(4, "dest", "carrier")
    .saveAsTable("flights_bucketed")  // just as long as a shuffle
  val flightsBucketed = spark.table("flights_bucketed")
  val mostDelayed2 = flightsBucketed
    .filter("origin = 'DEN' and arrdelay > 1")
    .groupBy("origin", "dest", "carrier")
    .avg("arrdelay")
    .orderBy($"avg(arrdelay)".desc_nulls_last)
  mostDelayed2.explain()
  /*
  == Physical Plan ==
  AdaptiveSparkPlan isFinalPlan=false
  +- Sort [avg(arrdelay)#160 DESC NULLS LAST], true, 0
     +- Exchange rangepartitioning(avg(arrdelay)#160 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=294]
        +- HashAggregate(keys=[origin#133, dest#130, carrier#124], functions=[avg(arrdelay#123)])
           +- HashAggregate(keys=[origin#133, dest#130, carrier#124], functions=[partial_avg(arrdelay#123)])
              +- Filter (isnotnull(arrdelay#123) AND (arrdelay#123 > 1.0))
                 +- FileScan parquet spark_catalog.default.flights_bucketed[arrdelay#123,carrier#124,dest#130,origin#133] Batched: true, Bucketed: true, DataFilters: [isnotnull(arrdelay#123), (arrdelay#123 > 1.0)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/madong/datahub-repository/spark-graphx/spark-warehouse/fli..., PartitionFilters: [isnotnull(origin#133), (origin#133 = DEN)], PushedFilters: [IsNotNull(arrdelay), GreaterThan(arrdelay,1.0)], ReadSchema: struct<arrdelay:double,carrier:string,dest:string>, SelectedBucketsCount: 4 out of 4
   */

  def main(args: Array[String]): Unit = {
//    joined.count()  // 4~5s
//    bucketedJoin.count()  // 4s for bucketing + 0.5s for counting
//    mostDelayed.show()  // ~1s
//    mostDelayed2.show() // ~0.2s = 5x perf!
  }

}
