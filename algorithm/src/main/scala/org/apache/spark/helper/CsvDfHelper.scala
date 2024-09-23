package org.apache.spark.helper

import org.apache.spark.graphx.Edge
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

  /** 从csv文件中读取实体点 */
  def getVertexRddFromCsv(spark: SparkSession, path: String) = {
    import spark.implicits._
    val csvDF = spark.read.format("csv")
      .options(Map(
        "header" -> "true",
        "inferSchema" -> "true"
      )).load(path)
    /*
     => 嫌疑人样例数据：
     18:11:21.444 [main] INFO org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator - Code generated in 9.520992 ms
      +--------+------+----------+
      |      id|  name|      type|
      +--------+------+----------+
      |10290101|徐志胜|st_suspect|
      |10290221|何广智|st_suspect|
      |10290921|  李诞|st_suspect|
      +--------+------+----------+

    => 案件样例数据：
    18:11:18.878 [main] INFO org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator - Code generated in 27.978234 ms
    +--------+-------------------+-------------------------------------+----------+-----+
    |      id|               name|                                 desc|      date| type|
    +--------+-------------------+-------------------------------------+----------+-----+
    |10290291|徐志胜-城北徐工孰美|     脱口秀内容～李诞｜何广致经典搭档|2024-09-18|st_aj|
    |10290102|      与辉同行-热点|董宇辉东方甄选与新东方俞敏洪的事件...|2023-06-18|st_aj|
    |10920128|   918-北京防空警报|        918勿忘国耻～民众爱过情怀高涨|2024-09-21|st_aj|
    +--------+-------------------+-------------------------------------+----------+-----+
    only showing top 3 rows
     */
    csvDF.show(3)

    csvDF.map(row => {
      val vid = row.getAs[Int]("id").toLong
      val vType = row.getAs[String]("type")
      if (vType.equals("st_aj"))
        (vid, Map(vid -> Set[Long]()))
      else
        (vid, Map[Long, Set[Long]]())
    })
  }

  /** 从csv文件中读取关系边 */
  def getEdgeRddFromCsv(spark: SparkSession, path: String) = {
    import spark.implicits._
    val edgeCsvDF = spark.read.options(Map(
      "header" -> "true",
      "inferSchema" -> "true"
    )).csv(path)
    /*
      :案件->嫌疑人3条关系边
      +--------+--------+--------+---------------+
      |      id|    from|      to|           type|
      +--------+--------+--------+---------------+
      |20290201|10290291|10290101|link_aj_suspect|
      |20290202|10290291|10290221|link_aj_suspect|
      |20291901|10290291|10290921|link_aj_suspect|
      +--------+--------+--------+---------------+
      only showing top 3 rows
     */
    edgeCsvDF.show(3)

    edgeCsvDF.map(row => {
      val fromId = row.getAs[Int]("from").toLong
      val toId = row.getAs[Int]("to").toLong
      val eType = row.getAs[String]("type")
      Edge(fromId, toId, eType)
    })
  }

}

