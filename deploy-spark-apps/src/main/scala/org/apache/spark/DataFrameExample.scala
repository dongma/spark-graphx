package org.apache.spark

import org.apache.spark.sql.SparkSession

/**
 * @author Sam Ma
 * @date 2020/07/13
 * 开发spark应用并在本地通过spark-submit提交jar包执行任务
 */
object DataFrameExample extends Serializable {

  /*
   * spark-submit脚本: $SPARK_HOME/bin/spark-submit --class org.apache.spark.DataFrameExample \
   *       --master=local \
   *       target/deploy-spark-apps-1.0.0.jar "/Users/madong/datahub-repository/spark-graphx/example-data"
   * success:
   * 20/07/14 00:32:30 INFO DAGScheduler: Job 1 finished: main at NativeMethodAccessorImpl.java:0, took 1.926153 s
     [spark deploy]
     [kubernetes deploy]
     20/07/14 00:32:30 INFO SparkContext: Invoking stop() from shutdown hook
   */
  def main(args: Array[String]): Unit = {
    val pathToDataFolder = args(0)
    /*
     * start up the sparkSession along with explicitly setting a given config
     */
    val spark = SparkSession.builder().appName("Spark Example")
      .config("spark.sql.warehouse.dir", "/Users/madong/datahub-repository/spark-graphx/spark-warehouse")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    // user define function registration
    spark.udf.register("lowercaseUdf", lowercase(_: String): String)
    val dataFrame = spark.read.json(pathToDataFolder + "/data.json")
    val manipulated = dataFrame.groupBy(expr("lowercaseUdf(group)")).sum().collect()
      .foreach(x => println(x))
  }

  def lowercase(word: String): String = {
    word.toLowerCase()
  }

}
