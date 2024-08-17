package query.plan

import org.apache.spark.sql.SparkSession

/**
 * Created by madong on 2024/8/17.
 */
object QueryPlans extends App {

  val spark = SparkSession.builder().appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val simpleNumbers = spark.range(1 , 100000)
  val times5 = simpleNumbers.selectExpr("id * 5 as id")
//  times5.explain()
  /*
  == Physical Plan ==
  *(1) Project [(id#0L * 5) AS id#2L]
  +- *(1) Range (1, 100000, step=1, splits=4)
  */

  val moreNumbers = spark.range(1, 1000000, 2)
  val split7 = moreNumbers.repartition(7)
//  split7.selectExpr("id * 5 as id").explain()
  /*
  == Physical Plan ==
  AdaptiveSparkPlan isFinalPlan=false
  +- Project [(id#0L * 5) AS id#4L]
     +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=9]
        +- Range (1, 1000000, step=2, splits=4)
  */

  val ds1 = spark.range(1, 10000000)
  val ds2 = spark.range(1, 20000000, 2)
  val ds3 = ds1.repartition(7)
  val ds4 = ds2.repartition(9)
  val ds5 = ds3.selectExpr("id * 3 as id")
  val joined = ds5.join(ds4, "id")
  val sum = joined.selectExpr("sum(id)")
  sum.explain
  /*
  == Physical Plan ==
  AdaptiveSparkPlan isFinalPlan=false
  +- HashAggregate(keys=[], functions=[sum(id#23L)])
     +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=90]
        +- HashAggregate(keys=[], functions=[partial_sum(id#23L)])
           +- Project [id#23L]
              +- SortMergeJoin [id#23L], [id#17L], Inner
                 :- Sort [id#23L ASC NULLS FIRST], false, 0
                 :  +- Exchange hashpartitioning(id#23L, 200), ENSURE_REQUIREMENTS, [plan_id=82]
                 :     +- Project [(id#15L * 3) AS id#23L]
                 :        +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=72]
                 :           +- Range (1, 10000000, step=1, splits=4)
                 +- Sort [id#17L ASC NULLS FIRST], false, 0
                    +- Exchange hashpartitioning(id#17L, 200), ENSURE_REQUIREMENTS, [plan_id=83]
                       +- Exchange RoundRobinPartitioning(9), REPARTITION_BY_NUM, [plan_id=75]
                          +- Range (1, 20000000, step=2, splits=4)
   */

}
