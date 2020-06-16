package org.apache.spark

import org.apache.spark.sql.SparkSession

/**
 * @author Sam Ma
 * @date 2020/06/16
 * spark SQL中的aggregations聚合和joins连接操作
 */
object AggregationsAndJoins {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("databricks spark application")
      .config("spark.master", "local")
      .getOrCreate()
    // 使用coalesce(numPartitions)指定了分区的数量，在大数量情况下指定分区数提高spark计算速度
    val retailAllDataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
      .load("/Users/madong/datahub-repository/spark-graphx/example-data/retail-data/all/*.csv")
      .coalesce(5)
    // 调用cache()方法会将整个dataframe缓存到内存中，调用retailAllDataFrame.count()方法会立即返回total count数量
    retailAllDataFrame.cache()
    retailAllDataFrame.createOrReplaceTempView("retailFullTable")
  }

}
