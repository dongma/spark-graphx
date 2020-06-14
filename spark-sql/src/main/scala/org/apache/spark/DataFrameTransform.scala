package org.apache.spark

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.functions.{expr, col, column, lit, desc, asc}

/**
 * @author Sam Ma
 * @date 2020/06/09
 * apache spark自定义schema用于load加载json文件到RDD中，常用的limit sort操作
 */
object DataFrameTransform {

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

    // 从dataframe的tempTable中查询给定字段("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME")的前两行记录
    flight2015Rdd.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)

    // 使用不同的方式从dataframe中提取字段数值，'DEST_COUNTRY_NAME'和$"DEST_COUNTRY_NAME"是无法通过编译
    flight2015Rdd.select(
      flight2015Rdd.col("DEST_COUNTRY_NAME"),
      col("DEST_COUNTRY_NAME"),
      column("DEST_COUNTRY_NAME"),
      //      'DEST_COUNTRY_NAME',
      //      $"DEST_COUNTRY_NAME",
      expr("DEST_COUNTRY_NAME")
    ).show(2)

    // 在spark SQL中使用glias重命名查询字段，可以使用alias()方法对查询到的字段"DEST_COUNTRY_NAME"进行重命名
    flight2015Rdd.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME")).show(2)
    // 使用selectExpr方法对DEST_COUNTRY_NAME进行重命名，可以添加DEST_COUNTRY_NAME参数作为字段重命名
    flight2015Rdd.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)
    // 在selectExpr中使用*表示所有的原始字段，对新增字段withinCountry可以写在参数列表之后
    flight2015Rdd.selectExpr("*", "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry").show(2)
    // 对count(distinct(DEST_COUNTRY_NAME))查找目的地的数量，avg(count)为所有数量的平均值
    flight2015Rdd.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)

    // 在数据表的所有列中添加numberOne字段numberOne，withColumn字段添加了额外的字段内容
    flight2015Rdd.withColumn("numberOne", lit(1)).show(2)
    val dfWithLongColName = flight2015Rdd.withColumn("This Long Column-Name", expr("ORIGIN_COUNTRY_NAME"))
    dfWithLongColName.selectExpr("`This Long Column-Name`", "`This Long Column-Name` as `new col`").show(2)
    dfWithLongColName.createOrReplaceTempView("dfTableLong")

    // 若要从dfTableLong移除字段，则可调用df.drop("ORIGIN_COUNTRY_NAME").columns方法
    //    flight2015Rdd.drop("ORIGIN_COUNTRY_NAME").columns
    // 使用filter进行数据过滤，也可以使用df.where("count < 2").show(2)语句进行数据筛选。在spark中使用多个where时可以chain调用
    flight2015Rdd.filter(col("count") < 2).show(2)
    flight2015Rdd.where("count < 2").show(2)
    flight2015Rdd.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia").show(2)

    // 在dataframe中使用distinct()方法对数据记录进行去重处理，相当于在sql中group by两个字段
    val distinct = flight2015Rdd.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
    logger.info(s"distinct ORIGIN_COUNTRY_NAME, DEST_COUNTRY_NAME field in flight2015Rdd, value: $distinct")

    // 使用randomSplit()方法对dataframe进行随机切分，生成两种不同的dataframe，其常用于machine learning
    val seed = 5
    val dataframes = flight2015Rdd.randomSplit(Array(0.25, 0.75), seed)
    logger.info("dataframes(0).count() > dataframes(1).count(): " + (dataframes(0).count() > dataframes(1).count()))

    // 使用union将多个dataframe拼接起来，在scala中dataframe是不可变化的，可使用concatenating或append拼接
    val dfSchema = flight2015Rdd.schema
    val newRows = Seq(
      Row("new country", "other country", 5L),
      Row("new country 2", "other country 3", 1L)
    )
    val parallelizedRows = spark.sparkContext.parallelize(newRows)
    val appendDf = spark.createDataFrame(parallelizedRows, dfSchema)
    // 在spark2.4版本中spark SQL查询筛选where条件无法通过$"ORIGIN_COUNTRY_NAME"获得引用的数据表字段
    flight2015Rdd.union(appendDf)
      .where("count = 1")
      .where(col("ORIGIN_COUNTRY_NAME") =!= "United States")
      .show()

    // 在spark sql dataframe中对字段排序时，可以使用sort()或者orderBy()对字段进行排序
    flight2015Rdd.sort("count").show(5)
    // 较高级的语法是使用asc_nulls_first, desc_nulls_first, asc_nulls_last或者desc_nulls_last来指定是否允许null值出现在DataFrame中
    flight2015Rdd.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).limit(2).show()

    // collectDf使用collect方法从整个DataFrame中获取数据，take(n)是从中获取n条记录
    val collectDf = flight2015Rdd.limit(10)
    collectDf.take(5)
    collectDf.show()
    collectDf.show(5, false)
    collectDf.collect()
  }

}
