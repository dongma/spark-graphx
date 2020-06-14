package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * @author Sam Ma
 * @date 2020/06/14
 * 在spark SQL中与不同的数据类型field进行协同用于过滤获取数据结果集
 */
object VariousTypeInSpark {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("databricks spark application")
      .config("spark.master", "local")
      .getOrCreate()

    // 通过sparkContext根据绝对路径从加载指定csv文件到dataframe中，同时创建临时表"dfRetailDataTable"数据类型转换
    val retailDataRdd = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
      .load("/Users/madong/datahub-repository/spark-graphx/example-data/retail-data/by-day/2010-12-01.csv")
    retailDataRdd.printSchema()
    retailDataRdd.createOrReplaceTempView("dfRetailDataTable")

    retailDataRdd.select(lit(5), lit("five"), lit(5.0))
    // 在spark中对于判断字段值是否相等可以使用===或者=!=，另一种直观的写法是在where("InvoiceNo" = 536365)
    retailDataRdd.where(col("InvoiceNo").equalTo(536365))
      .select("InvoiceNo", "Description")
      .show(5, false)

    // 若要在spark中通过and表达where筛选条件，则需使用chain方式将其串联起来 .where(filterOne).where(filterTwo)
    val priceFilter = col("UnitPrice") > 600
    val descripFilter = col("Description").contains("POSTAGE")
    retailDataRdd.where(col("StockCode").isin("DOT")).where(priceFilter.or(descripFilter)).show()

    // 为了过滤dataframe中的结果集可以指定一个Boolean columnName字段，在isExpensive字段上添加filter进行数据筛选
    val DOTCodeFilter = col("StockCode") === "DOT"
    retailDataRdd.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descripFilter)))
      .where("isExpensive")
      .select("unitPrice", "isExpensive").show(5)

    // 使用not()和col()函数的两种不同的用法，使用"isExpensive"字段对之前的结果集进行过滤，对于null字段的过滤可使用eqNullSafe("colName")过滤
    retailDataRdd.withColumn("isExpensive", not(col("UnitPrice").leq(250)))
      .filter("isExpensive")
      .select("Description", "UnitPrice").show(5)

    retailDataRdd.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))
      .filter("isExpensive")
      .select("Description", "UnitPrice").show(5)

    // 在spark SQL中与number类型进行数据集合的过滤,使用pow(value, 2)函数求给定数值的幂次方数值
    val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
    retailDataRdd.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)
    retailDataRdd.selectExpr("CustomerId",
      "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)
    // 使用round()函数对指定字段数值进行四舍五入计算，round()函数的第二个参数表示要保留小数的位数，bound()函数则与其相对对小数部分进行舍去
    retailDataRdd.select(round(col("UnitPrice"), 1).alias("rounded"), col("UnitPrice"))
      .show(5)

    // 使用corr()方法来计算两个字段之间的关联度correlation, if cheaper things are typically bought in greater quantities
    retailDataRdd.select(corr("Quantity", "UnitPrice")).show()
    // describe()方法会对dataframe中每个字段进行统计，求得其count总数、min最小值、max最大值etc
    retailDataRdd.describe().show()
    // 在查询结果集时可通过monotonically_increasing_id()方法对查询到的每一行记录添加一个unique唯一id标识
    retailDataRdd.select(monotonically_increasing_id()).show(2)

    // 从dataframe中对于给定字段使用lower() upper()方法进行字段大小写转换，ltrim() rtrim() trim()函数用于去除string字段中的空格
    retailDataRdd.select(initcap(col("Description")), lower(col("Description")), upper(col("Description")))
      .show(2)

    val simpleColors = Seq("black", "white", "red", "green", "blue")
    val regexString = simpleColors.map(_.toUpperCase).mkString("(", "|", ")")
    // 在dataframe中使用regexp_replace()方法进行正则表达式的替换，将Description中的"black"替换为"COLOR"值
    retailDataRdd.select(
      regexp_extract(col("Description"), regexString, 1).as("color_clean"),
      col("Description")).show(2)

    // 有时需要在field字段中查看某些常量字符串是否存在，可以通过contains方法进行数据过滤
    val containsBlack = col("Description").contains("BLACK")
    val containsWhite = col("DESCRIPTION").contains("WHITE")
    retailDataRdd.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
      .where("hasSimpleColor")
      .select("Description").show(3, false)

    // 使用动态参数在spark中进行查询，使用map将color转换为大写并对boolean字段进行重命名 col("Description").contains(color.toUpperCase).alias(s"is_white")
    val selectedColumns = simpleColors.map(color => {
      col("Description").contains(color.toUpperCase).alias(s"is_$color")
    }) :+ expr("*")
    // 在where筛选条件中只将col("is_white")和col("is_red")字段进行数据的筛选
    retailDataRdd.select(selectedColumns: _*).where(col("is_white").or(col("is_red")))
      .select("Description").show(3, false)

  }

}
