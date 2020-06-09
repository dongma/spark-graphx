package org.apache.spark

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.types.Metadata

/**
 * @author Sam Ma
 * @date 2020/06/09
 * apache spark自定义schema用于load加载json文件到RDD中，常用的limit sort操作
 */
object StructuredSchema {

  private[this] val logger = Logger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("databricks spark application")
      .config("spark.master", "local")
      .getOrCreate()

    val flight2015Rdd = spark.read.format("json")
      .load("/Users/madong/datahub-repository/spark-graphx/example-data/flight-data/json/2015-summary.json")
    flight2015Rdd.printSchema()

    // 自定义spark schema用于在DataFrame上加载数据，在schema中第一个字段名称，第二个表示字段类型，第三个表示是否可为null
    val manualSchema = StructType(Array(
      StructField("DEST_COUNTRY_NAME", StringType, true),
      StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      StructField("count", LongType, false, Metadata.fromJson("{\"hello\": \"world\"}"))
    ))

    // 使用自定义的schema约束从json文件中加载DataFrame的RDD数据, 通过在jsonDataFrame调用first()方法获取第一条记录
    val jsonDataFrame = spark.read.format("json").schema(manualSchema)
      .load("/Users/madong/datahub-repository/spark-graphx/example-data/flight-data/json/2015-summary.json")
    logger.info(s"jsonDataFrame value: $jsonDataFrame, custom rdd schema value: $manualSchema")
    val firstRecord = jsonDataFrame.first();
    println(s"first row in jsonDataFrame is: $firstRecord")

    // 在内存中创建临时表dfTable，通过Row()接口api向RDD中加入数据记录，通过调用spark的createDataFrame method
    jsonDataFrame.createOrReplaceTempView("dfTable")
    val myRows = Seq(Row("hello", null, 1L))
    val myRDD = spark.sparkContext.parallelize(myRows)
    val myDataFrame = spark.createDataFrame(myRDD, manualSchema)
    myDataFrame.show()
  }

}
