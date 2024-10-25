package org.apache.fundation

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Spark Catalyst优化，parquet文件的Pushing Filters
 *
 * @author Sam Ma
 * @date 2024/10/22
 */
object CatalystDemo {

  val spark = SparkSession.builder().appName("Catalyst Demo")
    .master("local").getOrCreate()

  import spark.implicits._

  val flights = spark.read.option("inferSchema", "true")
    .json("example-data/rtjvm/flights")

  val notFromHere = flights
    .where($"origin" =!= "LGA")
    .where($"origin" =!= "ORD")
    .where($"origin" =!= "SFO")
    .where($"origin" =!= "DEN")
    .where($"origin" =!= "BOS")
    .where($"origin" =!= "EWD")
  /* == Parsed Logical Plan ==
  'Filter NOT ('origin = EWD)
  +- Filter NOT (origin#19 = BOS)
     +- Filter NOT (origin#19 = DEN)
        +- Filter NOT (origin#19 = SFO)
           +- Filter NOT (origin#19 = ORD)
              +- Filter NOT (origin#19 = LGA)
                 +- Relation [_id#8,arrdelay#9,carrier#10,crsarrtime#11L,crsdephour#12L,crsdeptime#13L,crselapsedtime#14,depdelay#15,dest#16,dist#17,dofW#18L,origin#19] json

  == Analyzed Logical Plan ==
  _id: string, arrdelay: double, carrier: string, crsarrtime: bigint, crsdephour: bigint, crsdeptime: bigint, crselapsedtime: double, depdelay: double, dest: string, dist: double, dofW: bigint, origin: string
  Filter NOT (origin#19 = EWD)
  +- Filter NOT (origin#19 = BOS)
     +- Filter NOT (origin#19 = DEN)
        +- Filter NOT (origin#19 = SFO)
           +- Filter NOT (origin#19 = ORD)
              +- Filter NOT (origin#19 = LGA)
                 +- Relation [_id#8,arrdelay#9,carrier#10,crsarrtime#11L,crsdephour#12L,crsdeptime#13L,crselapsedtime#14,depdelay#15,dest#16,dist#17,dofW#18L,origin#19] json

   * == Optimized Logical Plan ==
  Filter (isnotnull(origin#19) AND ((NOT (origin#19 = LGA) AND NOT (origin#19 = ORD)) AND (((NOT (origin#19 = SFO) AND NOT (origin#19 = DEN)) AND NOT (origin#19 = BOS)) AND NOT (origin#19 = EWD))))
  +- Relation [_id#8,arrdelay#9,carrier#10,crsarrtime#11L,crsdephour#12L,crsdeptime#13L,crselapsedtime#14,depdelay#15,dest#16,dist#17,dofW#18L,origin#19] json

  == Physical Plan ==
  *(1) Filter ((((((isnotnull(origin#19) AND NOT (origin#19 = LGA)) AND NOT (origin#19 = ORD)) AND NOT (origin#19 = SFO)) AND NOT (origin#19 = DEN)) AND NOT (origin#19 = BOS)) AND NOT (origin#19 = EWD))
  +- FileScan json [_id#8,arrdelay#9,carrier#10,crsarrtime#11L,crsdephour#12L,crsdeptime#13L,crselapsedtime#14,depdelay#15,dest#16,dist#17,dofW#18L,origin#19] Batched: false, DataFilters: [isnotnull(origin#19), NOT (origin#19 = LGA), NOT (origin#19 = ORD), NOT (origin#19 = SFO), NOT (..., Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/Users/madong/datahub-repository/spark-graphx/example-data/rtjvm/..., Par
   */
  notFromHere.explain(true)

  def filterTeam1(flights: DataFrame) = flights.where($"origin" =!= "LGA").where($"origin" =!= "DEN")

  def filterTeam2(flights: DataFrame) = flights.where($"origin" =!= "EWR").where($"origin" =!= "DEN")

  val filterBoth = filterTeam1(filterTeam2(flights))
  filterBoth.explain(true)

  // pushing down filters
  flights.write.save("example-data/rtjvm/flights_parquet")
  val notFromLGA = spark.read.load("example-data/rtjvm/flights_parquet")
    .where($"origin" =!= "LGA")
  /*
    == Physical Plan ==
  *(1) Filter (isnotnull(origin#68) AND NOT (origin#68 = LGA))
  +- *(1) ColumnarToRow
   +- FileScan parquet [_id#57,arrdelay#58,carrier#59,crsarrtime#60L,crsdephour#61L,crsdeptime#62L,crselapsedtime#63,depdelay#64,dest#65,dist#66,dofW#67L,origin#68] Batched: true, DataFilters: [isnotnull(origin#68), NOT (origin#68 = LGA)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/madong/datahub-repository/spark-graphx/example-data/rtjvm/..., PartitionFilters: [], PushedFilters: [IsNotNull(origin), Not(EqualTo(origin,LGA))], ReadSchema: struct<_id:string,arrdelay:double,carrier:string,crsarrtime:bigint,crsdephour:bigint,crsdeptime:b...
  */
  notFromLGA.explain

  def main(args: Array[String]): Unit = {

  }

}
