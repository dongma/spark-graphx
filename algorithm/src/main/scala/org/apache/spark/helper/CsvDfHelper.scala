package org.apache.spark.helper

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 加载csv文件转换为Dataframe，可通过spark sql查数据
 *
 * @author Sam Ma
 * @date 2023/04/05
 */
class CsvDfHelper(spark: SparkSession) {

  /** 将csv文件注册为temporary view */
  def registerDf(name: String, dir: String): DataFrame = {
    val dataframe = loadDf(name, dir)
    dataframe.createOrReplaceTempView(name)
    dataframe
  }

  /** 加载csv文件为DataFrame, 读取csv文件header然后加载DataFrame */
  def loadDf(name: String, dir: String): DataFrame = {
    val schema = StructType(FieldMapping.getSchema(name).split(",").map(FieldMapping.getStructField))
    val dataframe = spark.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header", "true")
      .option("delimiter", ",")
      .schema(schema)
      .load("hdfs://127.0.0.1:9000" + dir + "/" + name + ".csv")
    dataframe
  }

}

object CsvDfHelper {

  /** 创建CsvDfHelper，在伴生对象中可直接使用 */
  def create(spark: SparkSession): CsvDfHelper = {
    new CsvDfHelper(spark)
  }

}

