package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

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

    // 在spark中处理日期date和时间戳timestamp类型的数据，并从dateframe中通过函数进行简单计算
    val dateRdd = spark.range(10).withColumn("today", current_date())
      .withColumn("now", current_timestamp())
    dateRdd.createOrReplaceTempView("dateTable")
    dateRdd.printSchema()
    dateRdd.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)
    // 使用datediff()函数计算两个日期间相差的天数，也可是使用alias对日期变量进行重命名，之后在比较函数中通过col进行引用
    dateRdd.withColumn("week_ago", date_sub(col("today"), 7))
      .select(datediff(col("week_ago"), col("today"))).show(1)
    dateRdd.select(
      to_date(lit("2016-01-01")).alias("start"),
      to_date(lit("2017-05-22")).alias("end")
    ).select(months_between(col("start"), col("end"))).show(1)
    // 使用dateFormat对日期进行格式化，将表常量字符串按照format格式转换为date类型，to_timestamp()函数要求指定format格式
    val dateFormat = "yyyy-dd-MM"
    val cleanDateDf = spark.range(1).select(
      to_date(lit("2017-12-11"), dateFormat).alias("date"),
      to_date(lit("2017-20-12"), dateFormat).alias("date2"))
    cleanDateDf.createOrReplaceTempView("dateTable2")
    cleanDateDf.select(to_timestamp(col("date"), dateFormat)).show()
    // 若要进行数据筛选可使用col("date2") > lit("2017-12-12")时间常量进行比较
    cleanDateDf.filter(col("date2") > lit("2017-12-12")).show()

    // spark中对于null值的处理，coalesce()函数用于从dateframe中找到第一个不为null的数据记录
    retailDataRdd.select(coalesce(col("Description"), col("CustomerId"))).show()
    // ifnull(null, 'return_value'), nullif('value1', 'value2'), nvl(null, 'return_value'), nvl2('not_null', 'return_value', 'else_value')
    retailDataRdd.na.drop("all", Seq("StockCode", "InvoiceNo"))
    // 使用fill()函数 可以通过指定一个map对数据列中给定字段设置默认值，对于null字段使用"StockCode"设置默认值5 对"Description"设置默认值"No Value"
    val fillColValues = Map("StockCode" -> 5, "Description" -> "No Value")
    retailDataRdd.na.fill(fillColValues)
    // replace()方法对于Description字段，当数据值为""时使用"UNKNOWN"字符串进行替换，对于ordering可以是asc_nulls_first, desc_nulls_first, asc_nulls_last or desc_nulls_last
    retailDataRdd.na.replace("Description", Map("" -> "UNKNOWN"))

    // 在dataframe中使用struct结构体类型，获取complex中的字段可以通过select("complex.Description")或(col("complex").getField("Description"))
    val complexDf = retailDataRdd.select(struct("Description", "InvoiceNo").alias("complex"))
    complexDf.createOrReplaceTempView("complexDf")
    complexDf.select("complex.Description").show(2)
    // 可以使用split() function指定分隔符来切分field字段, 使用alias()重新命名为新字段，然后通过下标索引获取值array_col[0]
    retailDataRdd.select(split(col("Description"), " ").alias("array_col"))
      .selectExpr("array_col[0]").show(2)
    // 可以通过size()方法获取得到切分array数组的长度，对于判断array中是否包含某一常量字符串 可使用array_contains()方法
    retailDataRdd.select(size(split(col("Description"), " "))).show(2)
    retailDataRdd.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)
    // explode()函数用于将为数组形式的field使用分割的item重新拼接数据行
    retailDataRdd.withColumn("splitted", split(col("Description"), " "))
      .withColumn("exploded", explode(col("splitted")))
      .select("Description", "InvoiceNo", "exploded").show(2)

    // 在dataframe中设置Map类型，使用col(filedName)对两个字段进行映射，第一个col(fieldName)作为key名称
    retailDataRdd.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
      .selectExpr("complex_map['WHITE METAL LANTERN']").show(2)
    // 可使用explode()函数对complex_map进行拆分，每一个key单独在一行中进行展示
    retailDataRdd.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
      .selectExpr("explode(complex_map)").show(2)

    // 可通过selectExpr()将json String作为jsonRdd数据，通过get_json_object()inline查询JSON对象，当json对象只嵌入一层时，使用json_tuple()方法
    val jsonDataFrame = spark.range(1).selectExpr("""
        '{"myJSONKey": {"myJSONValue": [1, 2, 3]}}' as jsonString""")
    jsonDataFrame.select(
      get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]") as "column",
      json_tuple(col("jsonString"), "myJSONKey")).show(2)
    // 可以将一个struct数据通过to_json()方法转换成为Json结构，使用from_json()可以将json转化为struct结构
    retailDataRdd.selectExpr("(InvoiceNo, Description) as myStruct").select(to_json(col("myStruct"))).show(2)
    val parseSchema = new StructType(Array(
      new StructField("InvoiceNo", StringType, true),
      new StructField("Description", StringType, true)
    ))
    retailDataRdd.selectExpr("(InvoiceNo, Description) as myStruct")
      .select(to_json(col("myStruct")).alias("newJSON"))
      .select(from_json(col("newJSON"), parseSchema), col("newJSON")).show(2)

    val udfExampleDataFrame = spark.range(5).toDF("num")
    def power3(number: Double): Double = number * number * number
    power3(2.0)
    val power3udf = udf(power3(_: Double): Double)
    udfExampleDataFrame.select(power3udf(col("num"))).show()
    // 将power3 udf注册到spark的context中，在ud的dataframe中可以调用power3(num)进行查询
    spark.udf.register("power3", power3(_: Double): Double)
    udfExampleDataFrame.selectExpr("power3(num)").show(2)
  }

}
