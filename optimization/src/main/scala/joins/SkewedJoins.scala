package joins

import generator.Generator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by madong on 2024/9/02.
 */
object SkewedJoins {

  val spark = SparkSession.builder().appName("Skewed Joins")
    .master("local[*]")
    .config("spark.sql.autoBroadcastJoinThreshold", -1) // deactivate broadcast joins
    .getOrCreate()
  import spark.implicits._

  /*
    An online store selling gaming laptops.
    2 laptops are "similar" if they have the same make & model, but proc speed within 0.1

    For each laptop configuration, we are interested in the average sale price of "similar" models,
    Acer Predator 2.9Ghz -> avg sale price of all acer Predators with CPU speed between 2.8 and 3.0Ghz
   */
  val laptops = Seq.fill(40000)(Generator.randomLaptop()).toDS()
  val laptopOffers = Seq.fill(100000)(Generator.randomLaptopOffer()).toDS()

  val joined = laptops.join(laptopOffers, Seq("make", "model"))
    .filter(abs(laptopOffers.col("procSpeed")) - laptopOffers.col("procSpeed") <= 0.1)
    .groupBy("registration")
    .agg(avg("salePrice").as("averagePrice"))
  /*
  == Physical Plan ==
  AdaptiveSparkPlan isFinalPlan=false
  +- HashAggregate(keys=[registration#4], functions=[avg(salePrice#20)])
     +- Exchange hashpartitioning(registration#4, 200), ENSURE_REQUIREMENTS, [plan_id=217]
        +- HashAggregate(keys=[registration#4], functions=[partial_avg(salePrice#20)])
           +- Project [registration#4, salePrice#20]
              +- SortMergeJoin [make#5, model#6], [make#17, model#18], Inner
                 :- Sort [make#5 ASC NULLS FIRST, model#6 ASC NULLS FIRST], false, 0
                 :  +- Exchange hashpartitioning(make#5, model#6, 200), ENSURE_REQUIREMENTS, [plan_id=209]
                 :     +- LocalTableScan [registration#4, make#5, model#6]
                 +- Sort [make#17 ASC NULLS FIRST, model#18 ASC NULLS FIRST], false, 0
                    +- Exchange hashpartitioning(make#17, model#18, 200), ENSURE_REQUIREMENTS, [plan_id=210]
                       +- LocalTableScan [make#17, model#18, salePrice#20]  */

  val laptops2 = laptops.withColumn("procSpeed", explode(array($"procSpeed" - 0.1, $"procSpeed", $"procSpeed" + 0.1)))
  val joined2 = laptops2.join(laptopOffers, Seq("make", "model", "procSpeed"))
    .groupBy("registration")
    .agg(avg("salePrice").as("averagePrice"))
  /*
  == Physical Plan ==
  AdaptiveSparkPlan isFinalPlan=false
  +- HashAggregate(keys=[registration#4], functions=[avg(salePrice#20)])
     +- Exchange hashpartitioning(registration#4, 200), ENSURE_REQUIREMENTS, [plan_id=232]
        +- HashAggregate(keys=[registration#4], functions=[partial_avg(salePrice#20)])
           +- Project [registration#4, salePrice#20]
              +- SortMergeJoin [make#5, model#6, knownfloatingpointnormalized(normalizenanandzero(procSpeed#43))], [make#17, model#18, knownfloatingpointnormalized(normalizenanandzero(procSpeed#19))], Inner
                 :- Sort [make#5 ASC NULLS FIRST, model#6 ASC NULLS FIRST, knownfloatingpointnormalized(normalizenanandzero(procSpeed#43)) ASC NULLS FIRST], false, 0
                 :  +- Exchange hashpartitioning(make#5, model#6, knownfloatingpointnormalized(normalizenanandzero(procSpeed#43)), 200), ENSURE_REQUIREMENTS, [plan_id=224]
                 :     +- Generate explode(array((procSpeed#7 - 0.1), procSpeed#7, (procSpeed#7 + 0.1))), [registration#4, make#5, model#6], false, [procSpeed#43]
                 :        +- LocalTableScan [registration#4, make#5, model#6, procSpeed#7]
                 +- Sort [make#17 ASC NULLS FIRST, model#18 ASC NULLS FIRST, knownfloatingpointnormalized(normalizenanandzero(procSpeed#19)) ASC NULLS FIRST], false, 0
                    +- Exchange hashpartitioning(make#17, model#18, knownfloatingpointnormalized(normalizenanandzero(procSpeed#19)), 200), ENSURE_REQUIREMENTS, [plan_id=225]
                       +- LocalTableScan [make#17, model#18, procSpeed#19, salePrice#20]
   */

  def main(args: Array[String]): Unit = {
    joined2.show()
    joined2.explain()
    Thread.sleep(1000000)
  }

}
