package org.apache.spark.jobs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}

/**
 * New York City Taxi data Analysis
 *
 * @author Sam Ma
 * @date 2024/05/18
 */
object TaxiApplication extends App {

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Taxi Big Data Application")
    .getOrCreate()
  import spark.implicits._

  // ~ New York City出租车运行数据
  val bigTaxiDF = spark.read.load("NYC_taxi_2009-2016.parquet")
  val taxiDF = spark.read.load("yellow_taxi_jan_25_2018")
  taxiDF.printSchema()

  val taxiZonesDF = spark.read.option("header", "true")
    .option("inferSchema", "true").csv("rtjvm/taxi_zones.csv")
  taxiZonesDF.printSchema()

  /**
   * Questions:
   * 1、which zones have the most pickups/dropoffs overall?
   * 2、what are the peak hours for taxi?
   * 3、How are the trips distributed? why are people talking the cab?
   * 4、what are the peak hours for long/short trips?
   * 5、what are the top 3 pickup/dropoff zones for long/short trips?
   * 6、how are people paying for the ride, on long/short trips?
   * 7、how is the payment type evolving with time?
   * 8、Can we explore a ride-sharing opportunity by grouping close short trips?
   */
  // 1
  val pickupsByTaxiZoneDF = taxiDF.groupBy("PULocationID")
    .agg(count("*").as("totalTrips"))
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID", "service_zone")
    .orderBy(col("totalTrips").desc_nulls_last)
  pickupsByTaxiZoneDF.show()

  // 1b - group by Borough
  val pickupsByThrough = pickupsByTaxiZoneDF.groupBy(col("Borough"))
    .agg(sum(col("totalTrips")).as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)
  pickupsByThrough.show()

  // 2
  val pickupsByHourDF = taxiDF.withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day")
    .agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)
  pickupsByHourDF.show()

  // 3
  val tripDistanceDF = taxiDF.select(col("trip_distance").as("distance"))
  val longDistanceThreshold = 30
  val tripDistanceStatsDF = tripDistanceDF.select(
    count("*").as("count"),
    lit(longDistanceThreshold).as("threshold"),
    mean("distance").as("mean"),
    stddev("distance").as("stddev"),
    min("distance").as("min"),
    max("distance").as("max")
  )
  tripDistanceStatsDF.show()

  val tripsWithLengthDF = taxiDF.withColumn("isLong", col("trip_distance") >= longDistanceThreshold)
  val tripsByLengthDF = tripsWithLengthDF.groupBy("isLong").count()

  // 4
  val pickupsByHourByLengthDF = tripsWithLengthDF
    .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day", "isLong")
    .agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)
  pickupsByHourByLengthDF.show(48)

  // 5
  def pickupDropoffPopularityDF(predicate: Column) = tripsWithLengthDF
    .where(predicate)
    .groupBy("PULocationID", "DOLocationID").agg(count("*").as("totalTrips"))
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .withColumnRenamed("Zone", "Pickup_Zone")
    .drop("LocationID", "Borough", "service_zone")
    .join(taxiZonesDF, col("DOLocationID") === col("LocationID"))
    .withColumnRenamed("Zone", "Dropoff_Zone")
    .drop("LocationID", "Borough", "service_zone")
    .drop("PULocationID", "DOLocationID")
    .orderBy(col("totalTrips").desc_nulls_last)

  pickupDropoffPopularityDF(col("isLong")).show
  pickupDropoffPopularityDF(not(col("isLong"))).show

  /**
   * Questions:
   *
   * 6、how are people paying for the ride, on long/short trips?
   * 7、how is the payment type evolving with time?
   * 8、Can we explore a ride-sharing opportunity by grouping close short trips?
   */
  // 6
  val ratecodeDistributionDF = taxiDF
    .groupBy(col("RatecodeID")).agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  // 7
  val ratecodeEvolution = taxiDF
    .groupBy(to_date(col("tpep_pickup_datetime")).as("pickup_day"), col("RatecodeID"))
    .agg(count("*").as("totalTrips"))
    .orderBy(col("pickup_day"))

  // 8
  val groupAttemptsDF = taxiDF
    .select(round(unix_timestamp(col("tpep_pickup_datetime")) / 300)
      .cast("integer").as("fiveMidId"),
      col("PULocationID"),
      col("total_amount"))
    .where(col("passenger") < 3)
    .groupBy(col("fiveMidId"), col("PULocationID"))
    .agg(count("*").as("total_trips"), sum(col("total_amount")).as("total_amount"))
    .orderBy(col("total_trips").desc_nulls_last)
    .withColumn("approximate_datetime", from_unixtime(col("fiveMinId") * 300))
    .drop("fiveMinId")
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID", "service_zone")
  groupAttemptsDF.show()

  val percentGroupAttempt = 0.05
  val percentAcceptGrouping = 0.3
  val discount = 5
  val extraCost = 2
  val avgCostReduction = 0.6 * taxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0)

  val groupingEstimateEconomicImpactDF = groupAttemptsDF
    .withColumn("groupedRides", col("total_trips") * percentGroupAttempt)
    .withColumn("acceptedGroupedRidesEconomicImpact",
      col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discount))
    .withColumn("rejectedGroupedRidesEconomicImpact",
      col("groupedRides") * (1 - percentAcceptGrouping) * extraCost)
    .withColumn("totalImpact", col("acceptedGroupedRidesEconomicImpact")
      + col("rejectedGroupedRidesEconomicImpact"))
  groupingEstimateEconomicImpactDF.show(100)

  val totalProfitDF = groupingEstimateEconomicImpactDF.select(sum(col("totalImpact")).as("total"))
  // 40k/day = 12 million/year!!!
  totalProfitDF.show()

}
