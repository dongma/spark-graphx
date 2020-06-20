package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

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

    // 使用count(*)时spark的统计会包含null数据，当直接使用字段count("StockCode")进行统计会排除掉null数据，对于字段去重使用countDistinct方法
    retailAllDataFrame.select(count("StockCode")).show()
    retailAllDataFrame.select(countDistinct("StockCode")).show()
    // 可用approx_count_distinct()函数计算关联度
    retailAllDataFrame.select(approx_count_distinct("StockCode", 0.1)).show()
    // first()和last()函数用于获取dataframe中的第一行和最后一行的数据字段，min()和max()用于获取该字段的最大值、最小值
    retailAllDataFrame.select(first("StockCode"), last("StockCode")).show()
    retailAllDataFrame.select(min("Quantity"), max("Quantity")).show()
    // sum()统计给定字段所有数值总和，sumDistinct()用于统计该字段去重数据的总和
    retailAllDataFrame.select(sum("Quantity"), sumDistinct("Quantity")).show()
    // 通过avg()函数统计字段的平均值，avg()=sum()/count(), mean(Quantity)的计算结果和avg(field)是相同的
    retailAllDataFrame.select(
      count("Quantity").alias("total_transactions"),
      sum("Quantity").alias("total_purchases"),
      avg("Quantity").alias("avg_purchases"),
      expr("mean(Quantity)").alias("mean_purchases"))
      .selectExpr(
        "total_purchases/total_transactions",
        "avg_purchases",
        "mean_purchases").show()

    // variance() and standard deviation function, the variance is the average of the squared differences from the mean
    retailAllDataFrame.select(var_pop("Quantity"), var_samp("Quantity"),
      stddev_pop("Quantity"), stddev_samp("Quantity")).show()
    // skewness and kurtosis are both measurements of extreme points in your data
    retailAllDataFrame.select(skewness("Quantity"), kurtosis("Quantity")).show()
    // for covariance and correlation, respectively. Correlation measures the pearson correlation coefficient, which is between -1 and +1
    retailAllDataFrame.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),
      covar_pop("InvoiceNo", "Quantity")).show()
    // aggregating to complex types, you can perform aggregations not just of numerical values using formulas, also perform them on complex types
    retailAllDataFrame.agg(collect_set("Country"), collect_list("Country")).show()

    // 对dataframe中的数据按照字段进行分组("InvoiceNo"、"CustomerId")
    retailAllDataFrame.groupBy("InvoiceNo", "CustomerId").count().show()
    // grouping with expressions,expr("count(Quantity)")的效果与count("Quantity")相同
    retailAllDataFrame.groupBy("InvoiceNo").agg(
      count("Quantity").alias("quan"),
      expr("count(Quantity)")).show()
    // 使用一系列的map作为dataframe的数据分组，列的名称为avg("Quantity") 行的名称为stddev_pop("Quantity")
    retailAllDataFrame.groupBy("InvoiceNo").agg("Quantity" -> "avg", "Quantity" -> "stddev_pop").show()

    // 可以使用window()函数在获取得到一些unique的统计在一些specific window of data, 其概念有一些抽象 similar to standard group-by
    val dfWithDate = retailAllDataFrame.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
    dfWithDate.createOrReplaceTempView("dfWithDate")
    // create a window specification function, partition by is unrelated to the partitioning scheme concept that we have covered thus far
    val windowSpec = Window.partitionBy("CustomerId", "date").orderBy(col("Quantity").desc)
      // rowsBetween states which rows will be included in the frame based on its reference
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
    // use dense_rank() function to determine which date had the maximum purchase quantity for every customer
    val purchaseDenseRank = dense_rank().over(windowSpec)
    val purchaseRank = rank().over(windowSpec)
    dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")
      .select(
        col("CustomerId"),
        col("date"),
        col("Quantity"),
        purchaseRank.alias("quantityRank"),
        purchaseDenseRank.alias("quantityDenseRank"),
        maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()

    // grouping set which is an aggregation across multiple groups, we achieve this by using grouping sets
    val dfNotNull = dfWithDate.drop()
    dfNotNull.createOrReplaceTempView("dfNotNull")
    // the grouping sets operator is only available in spark SQL, 在grouping sets()中可指定多个不同分组
/*    select customerId, stockcode, sum(quantity) from dfNotNull
      group by customerId, stockCode grouping sets((customerId, stockCode), ())
      order by customerId desc, stockCode desc*/
    // a rollup is multidimensional aggregation that performs a variety of group-by style calculations for us
    val rolledUpDf = dfNotNull.rollup("Date", "Country").agg(sum("Quantity"))
      .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
      .orderBy("Date")
    rolledUpDf.show()
    rolledUpDf.where("Country IS NULL").show()
    // a cube does the same things across all dimensions, this means that it won't just go by date over the entire time
    dfNotNull.cube("Date", "Country").agg(sum(col("Quantity")))
      .select("Date", "Country", "sum(Quantity)").orderBy("Date").show()
    // grouping metadata
    dfNotNull.cube("customerId", "stockCode").agg(grouping_id(), sum("Quantity"))
      .orderBy(expr("grouping_id()").desc)
      .show()
    // pivots make is possible for you to convert a row into a column
    val piovted = dfWithDate.groupBy("date").pivot("Country").sum()
    piovted.where("date > '2011-12-05'").select("date", "`USA_sum(Quantity)`").show()

    // apache spark对user defined function进行注册，目前在spark 2.4.5版本上执行register udf函数是存在问题的
    /*val boolAnd = new BooleanAnd()
    spark.udf.register("booland", boolAnd)
    spark.range(1).selectExpr("explode(array(TRUE, TRUE, TRUE)) as t")
      .selectExpr("explode(array(TRUE, FALSE, TRUE)) as f", "t")
      .select(boolAnd(col("t")), expr("booland(f)"))
      .show()*/


    import spark.sqlContext.implicits._
    // 通过Seq(...).toDF(columnName1, columnName2, columnName3)创建DataFrame，需导入spark.sqlContext.implicits内容
    val personDataFrame = Seq(
      (0, "Bill Chambers", 0, Seq(100)),
      (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
      (2, "Michael Armbrust", 1, Seq(250, 100))
    ).toDF("id", "name", "graduate_program", "spark_status")
    personDataFrame.createOrReplaceTempView("person")
    val graduateProgramDf = Seq(
      (0, "Masters", "School of Information", "UC Berkeley"),
      (2, "Masters", "EECS", "UC Berkeley"),
      (1, "PH.D", "EECS", "UC Berkeley")
    ).toDF("id", "degree", "department", "school")
    graduateProgramDf.createOrReplaceTempView("graduateProgram")
    val sparkStatusDf = Seq(
      (500, "Vice President"),
      (250, "PMC Member"),
      (100, "Contributor")).toDF("id", "status")
    sparkStatusDf.createOrReplaceTempView("sparkStatus")

    // innerjoin类型 可以指定joinType类型，同时指定joinType类型为inner join
    val joinExpression = personDataFrame.col("graduate_program") === graduateProgramDf.col("id")
    var joinType = "inner"
    personDataFrame.join(graduateProgramDf, joinExpression, joinType).show()

    // outer joins evaluate the keys in both of the dataframes or tables, if there is no equivalent row exists, spark will insert null
    joinType = "outer"
    personDataFrame.join(graduateProgramDf, joinExpression, joinType).show()

    // 左外连接left_outer类型，左外连接以左表为主 若不存在对应的key，则spark会直接插入null(right_outer则正好相反)
    joinType = "left_outer"
    graduateProgramDf.join(personDataFrame, joinExpression, joinType).show()
    joinType = "right_outer"
    personDataFrame.join(graduateProgramDf, joinExpression, joinType).show()

    // left_semi join类型为当左表中的数据在right table中存在时，则将其保存在result中
    joinType = "left_semi"
    graduateProgramDf.join(personDataFrame, joinExpression, joinType).show()
    // 对两张数据表使用cross join进行连接，cross join will join every single row in the left DataFrame to ever single row in the right DataFrame
    joinType = "cross"
    graduateProgramDf.join(personDataFrame, joinExpression, joinType).show()

    // joins on complex types，使用inner join在筛选条件部分 使用array_contains(spark_status, id)谓词进行数据筛选
    personDataFrame.withColumnRenamed("id", "personId")
      .join(sparkStatusDf, expr("array_contains(spark_status, id)")).show()
    // 为避免在spark sql中不同dataframe中存在相同column问题，可以使用不同的join type、或者join之后drop同名的列、join前renaming一列
    val gradProgramDupe = graduateProgramDf.withColumnRenamed("id", "graduate_program")
    var joinExpr = gradProgramDupe.col("graduate_program") === personDataFrame.col("graduate_program")
//    personDataFrame.join(gradProgramDupe, joinExpr).select("graduate_program").show()
    personDataFrame.join(gradProgramDupe, "graduate_program").select("graduate_program").show()
    // 同名的columnName在join之后可以删除任意一个column，这样在后期select时不会由于column重复产生语法解析错误
    personDataFrame.join(gradProgramDupe, joinExpr).drop(personDataFrame.col("graduate_program"))
      .select("graduate_program").show()

    joinExpr = personDataFrame.col("graduate_program") === graduateProgramDf.col("id")
    personDataFrame.join(graduateProgramDf, joinExpr).explain()
/*    == Physical Plan ==
      *(1) BroadcastHashJoin [graduate_program#8096], [id#8111], Inner, BuildLeft
    :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[2, int, false] as bigint)))
      :  +- LocalTableScan [id#8094, name#8095, graduate_program#8096, spark_status#8097]
    +- LocalTableScan [id#8111, degree#8112, department#8113, school#8114]*/

  }

}
