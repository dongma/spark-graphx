package org.apache.spark

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Spark SQL Programming
 *
 * @author Sam Ma
 * @date 2024/04/27
 */
object SparkSql extends App {

  val spark = SparkSession.builder().appName("Spark SQL Practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "spark-warehouse")
    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()
  val carsDF = spark.read.option("inferSchema", "true")
    .json("example-data/rtjvm/cars.json")

  // regular DF API
  carsDF.select(col("Name")).where(col("Origin") === "USA")
  // Use Spark SQL
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      | select Name from cars where Origin = 'USA'
    """.stripMargin)
  americanCarsDF.show()

  // we can run any SQL statement
  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  val databasesDF = spark.sql("show databases")

  // transfer tables from a DB to Spark tables
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  /** 使用jdbc从postgresql数据库中读取数据表 */
  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  /** 传输数据表，将DF中的数据写到spark warehouse中 */
  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false) =
    tableNames.foreach { tableName =>
      val tableDF = readTable(tableName)
      tableDF.createOrReplaceTempView(tableName)

      if (shouldWriteToWarehouse) {
        tableDF.write
          .mode(SaveMode.Overwrite)
          .saveAsTable(tableName)
      }
    }

  transferTables(List(
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager")
  )

  // read DF from loaded spark tables
  val employeesDF2 = spark.read.table("employees")

  /**
   * Exercises:
   * 1.Read the movies DF and store it as a Spark table in the rtjvm database.
   * 2.Count how many employees were hired in between Jan 1 1999 and Jan 1 2001.
   * 3.Show the average salaries for the employees hired in between those dates, grouped by department.
   * 4.Show the name of the best-paying department for employees hired between those dates.
   */

  // 1
  val moviesDF = spark.read.option("inferSchema", "true")
    .json("example-data/rtjvm/movies.json")
//  moviesDF.write
//    .mode(SaveMode.Overwrite)
//    .saveAsTable("movies")

  // 2
  spark.sql(
    """
      | select count(*)
      | from employees
      | where hire_date > '1999-01-01' and hire_date < '2001-01-01'
      |""".stripMargin
  ).show()

  // 3
  spark.sql(
    """
      | select de.dept_no, avg(s.salary)
      | from employees e, dept_emp de, salaries s
      | where e.hire_date > '1999-01-01' and e.hire_date < '2001-01-01'
      |   and e.emp_no = de.emp_no
      |   and e.emp_no = s.emp_no
      | group by de.dept_no
      |""".stripMargin
  ).show()

  // 4
  spark.sql(
    """
      | select avg(s.salary) payments, d.dept_name
      | from employees e, dept_emp de, salaries s, departments d
      | where e.hire_date > '1999-01-01' and e.hire_date < '2001-01-01'
      |   and e.emp_no = de.emp_no
      |   and e.emp_no = s.emp_no
      |   and de.dept_no = d.dept_no
      | group by d.dept_name
      | order by payments desc
      | limit 1
      |""".stripMargin
  ).show()

}
