package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory

/**
 * @author Sam Ma
 * @date 2020/06/20
 * apache spark与各种数据源的交互，包括csv json Parquet Jdbc connection etc
 */
object DataSourcesInSpark {

  private val logger = LoggerFactory.getLogger(DataSourcesInSpark.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("databricks spark application")
      .config("spark.master", "local")
      .getOrCreate()

    val customSchema = new StructType(Array(
      new StructField("DEST_COUNTRY_NAME", StringType, true),
      new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      new StructField("count", LongType, false)
    ))

    val summaryDf = spark.read.format("csv").option("header", "true")
      .option("mode", "FAILFAST")
      .schema(customSchema)
      .load("/Users/madong/datahub-repository/spark-graphx/example-data/flight-data/csv/2010-summary.csv")
//      .show(5)
    // 使用DataframeWriter Api将df中的数据转换为csv格式内容并进行输出，或者也可以在save()中指定要保存的路径
    summaryDf.write.format("csv").option("mode", "OVERWRITE")
      .option("dateFormat", "yyyy-MM-dd")
      .option("path", "/tmp/dataframe-output-data")
      .mode("overwrite")
      .save()

    // 对json格式数据的文件的处理，在spark.read.format()中指定文件格式为json
    val jsonSummaryDf = spark.read.format("json").option("mode", "FAILFAST").schema(customSchema)
      .load("/Users/madong/datahub-repository/spark-graphx/example-data/flight-data/json/2010-summary.json")
//      .show(5)
    jsonSummaryDf.write.format("json")
      .mode("overwrite")
      .save("/tmp/dataframe-output-data-json")

    // reading from MySQL database，和正常mysql driver连接相同也需要指定url driver等信息，使用numPartitions参数并行读数据
    val mysqlSummaryDf = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://127.0.0.1:3306/spark_datasource?useUnicode=true&characterEncoding=utf8")
      .option("dbtable", "summary_2010")
      .option("user", "root").option("password", "123456")
      .option("numPartitions", 10)
      .load()
    mysqlSummaryDf.select("DEST_COUNTRY_NAME").distinct().show(5)

    val pushdownQuery = """(select distinct(DEST_COUNTRY_NAME) from summary_2010) as desc_country_summary"""
    val dbDataFrame = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://127.0.0.1:3306/spark_datasource?useUnicode=true&characterEncoding=utf8")
      .option("dbtable", pushdownQuery)
      .option("user", "root").option("password", "123456").load()
    dbDataFrame.show()

    // apache spark-sql 将dataframe转换为sql数据表
    spark.read.json("/Users/madong/datahub-repository/spark-graphx/example-data/flight-data/json/2015-summary.json")
      .createOrReplaceTempView("some_sql_view")
    val countryCount = spark.sql("""
        select DEST_COUNTRY_NAME, sum(count) from some_sql_view group by DEST_COUNTRY_NAME
        """).where("DEST_COUNTRY_NAME like 'S%'").where("`sum(count)` > 10")
      .count()
    logger.info(s"$countryCount dest_country which the name is start with S")

  }

}
