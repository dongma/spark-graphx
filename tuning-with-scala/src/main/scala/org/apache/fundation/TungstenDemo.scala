package org.apache.fundation

import org.apache.spark.sql.SparkSession

/**
 * Spark Tungsten优化
 *
 * @author Sam Ma
 * @date 2024/10/23
 */
object TungstenDemo {

  val spark = SparkSession.builder().appName("Tungsten Demo")
    .master("local").getOrCreate()
  val sc = spark.sparkContext

  val numbersRDD = sc.parallelize(1 to 10000000).cache()
  numbersRDD.count()
  numbersRDD.count()  // much faster

  import spark.implicits._
  val numbersDF = numbersRDD.toDF("value").cache()  // cached with Tungsten
  numbersDF.count()
  numbersDF.count() // much faster

  /* 22:15:56.478 [main] DEBUG org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec - Final plan:
  *(2) HashAggregate(keys=[], functions=[count(1)], output=[count#14L])
  +- ShuffleQueryStage 1
     +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=49]
        +- *(1) HashAggregate(keys=[], functions=[partial_count(1)], output=[count#32L])
           +- TableCacheQueryStage 0
              +- InMemoryTableScan
                    +- InMemoryRelation [value#5], StorageLevel(disk, memory, deserialized, 1 replicas)
                          +- *(1) SerializeFromObject [input[0, int, false] AS value#2]
                             +- Scan[obj#1]

  22:15:56.521 [main] DEBUG org.apache.spark.sql.execution.adaptive.InsertAdaptiveSparkPlan - Adaptive execution enabled for plan: HashAggregate(keys=[], functions=[count(1)], output=[count#45L])
  +- HashAggregate(keys=[], functions=[partial_count(1)], output=[count#63L])
     +- InMemoryTableScan
           +- InMemoryRelation [value#5], StorageLevel(disk, memory, deserialized, 1 replicas)
                 +- *(1) SerializeFromObject [input[0, int, false] AS value#2]
                    +- Scan[obj#1] */
  // Tungsten is active in wholeStageCodegen
  spark.conf.set("spark.sql.codegen.wholeStage", "false")
  val noWholeStageSum = spark.range(1000000).selectExpr("sum(id)")
  noWholeStageSum.explain()
  noWholeStageSum.show()

  /*
  22:20:59.515 [main] DEBUG org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec - Final plan:
  *(1) HashAggregate(keys=[], functions=[sum(id#86L)], output=[toprettystring(sum(id))#94])
  +- *(1) HashAggregate(keys=[], functions=[partial_sum(id#86L)], output=[sum#92L])
     +- *(1) Range (0, 1000000, step=1, splits=1)
   */
  spark.conf.set("spark.sql.codegen.wholeStage", "true")
  val wholeStageSum = spark.range(1000000).selectExpr("sum(id)")
  wholeStageSum.explain()
  wholeStageSum.show()

  def main(args: Array[String]): Unit = {
    Thread.sleep(1000000)
  }

}
