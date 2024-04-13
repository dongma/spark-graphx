package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

/**
 * DataFrame使用col和expr等操作具体字段
 *
 * @author Sam Ma
 * @date 2024/04/13
 */
object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder().appName("DF columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read.option("inferSchema", "true")
    .json("example-data/rtjvm/cars.json")
  // Columns
  val firstColumn = carsDF.col("Name")

  // selecting (projecting)
  val carNamesDF = carsDF.select(firstColumn)
  carNamesDF.show(10)

  // various select methods
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala symbol, auto-converted to column
    $"Horsepower", // fancier interpolated string, return a Column object
    expr("Origin") // EXPRESSION
  )

  // select with plain column names
  carsDF.select("Name", "Year")

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 3.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("weightInKg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )
  carsWithWeightsDF.show(10)

  // selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing, adding a column，rename a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // careful with column names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")
  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // unioning = adding more rows
  val moreCarsDF = spark.read.option("inferSchema", "true").json("example-data/rtjvm/cars_dates.json")
  val allCarsDF = carsDF.union(moreCarsDF)  // works if the DFs have the same schema

  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show(10)
}
