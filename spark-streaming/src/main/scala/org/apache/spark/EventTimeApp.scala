package org.apache.spark

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.GroupState
import org.apache.spark.state.processing.{InputRow, UserState}
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author Sam Ma
 * @date 2020/07/22
 * EventTime Processing and Stateful Processing in DStream API
 */
object EventTimeApp {

  private[this] val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("EventTime app")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", "/Users/madong/datahub-repository/spark-graphx/spark-warehouse")
      .config("spark.sql.streaming.schemaInference", "true")
      // 设置数据处理分区数partitions，避免在local master产生过多分区
      .config("spark.sql.shuffle.partitions", 5)
      .getOrCreate()

    val static = spark.read.json("/Users/madong/datahub-repository/spark-graphx/example-data/activity-data")
    val streaming = spark.readStream.schema(static.schema)
      .option("maxFilesPerTrigger", 10)
      .json("/Users/madong/datahub-repository/spark-graphx/example-data/activity-data")
    streaming.printSchema()

    // 将json数据中的Creation_time字段转换成为timestamp类型，可根据window()获取streaming记录
    val withEventTime = streaming.selectExpr(
      "*",
      "cast(cast(Creation_time as double)/1000000000 as timestamp) as event_time")

    import org.apache.spark.sql.functions.{window, col}
    val eventPerWindowQuery = withEventTime
      // .withWatermark() method specify how late we expect to see data, freeing those objects after timeout
      .withWatermark("event_time", "5 hours")
      .dropDuplicates("event_time")
      .groupBy(window(col("event_time"), "10 minutes", "5 minutes")).count()
      .writeStream
      .queryName("events_per_window")
      .format("memory")
      .outputMode("complete")
      .start()

    /* root
     |-- window: struct (nullable = false)
     |    |-- start: timestamp (nullable = true)
     |    |-- end: timestamp (nullable = true)
     |-- count: long (nullable = false)
     */
    spark.sql("select * from events_per_window").printSchema()

    /*+--------------------+-----+
      |              window|count|
      +--------------------+-----+
      |[2015-02-23 18:40...| 1104|
      |[2015-02-24 19:50...| 1849|
      +--------------------+-----+
     */
    eventPerWindowQuery.awaitTermination(timeoutMs = 5000)
    spark.sql("select * from events_per_window").show(5, false)

    // spark streaming GroupStateWithTimeout, this is similar to a user-defined-aggregation function
    import spark.sqlContext.implicits._
    import org.apache.spark.sql.streaming.GroupStateTimeout
    val eventPerWindowWithStateQuery = withEventTime
      .selectExpr("User as user",
      "cast(Creation_Time/1000000000 as timestamp) as timestamp", "gt as activity")
      .as[InputRow]
      .groupByKey(_.user)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateAcrossEvents)
      .writeStream
      .queryName("events_per_window_state")
      .format("memory")
      .outputMode("update")
      .start()
    eventPerWindowWithStateQuery.awaitTermination(timeoutMs = 5000)
    logger.info("spark streaming GroupStateWithTimeout config, ")
    spark.sql("select * from events_per_window_state").show(5, false)

  }

  /**
   * set up the function that defines how you will update your state based on a given row
   * @param state
   * @param input
   * @return
   */
  def updateUserStateWithEvent(state: UserState, input: InputRow): UserState = {
    if (Option(input.timestamp).isEmpty) {
      return state
    }
    if (state.activity == input.activity) {
      if (input.timestamp.after(state.end)) {
        state.end = input.timestamp
      }
      if (input.timestamp.before(state.start)) {
        state.start = input.timestamp
      }
    } else {
      if (input.timestamp.after(state.end)) {
        state.start = input.timestamp
        state.end = input.timestamp
        state.activity = input.activity
      }
    }
    state
  }

  /**
   * write the function that defines the way state is updated based on epoch of rows
   * @param user
   * @param inputs
   * @param oldState
   * @return
   */
  def updateAcrossEvents(user: String, inputs: Iterator[InputRow],
                         oldState: GroupState[UserState]): UserState = {
    var state: UserState = if (oldState.exists) oldState.get else UserState(
      user,
      "",
      new Timestamp(6284160000000L),
      new Timestamp(6284160L)
    )
    // we simply specify an old date that we can compare against and immediately update
    // based on the values in our data

    for (inputRow <- inputs) {
      state = updateUserStateWithEvent(state, inputRow)
      oldState.update(state)
    }
    state
  }

}
