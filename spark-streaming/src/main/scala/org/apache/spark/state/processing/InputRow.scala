package org.apache.spark.state.processing

/**
 * @author Sam Ma
 * @date 2020/07/24
 * InputRow表示输入行中的数据
 */
case class InputRow(user: String, timestamp: java.sql.Timestamp, activity: String)

/*
 * UserState为流式数据处理结果
 */
case class UserState(user: String, var activity: String,
                     var start: java.sql.Timestamp,
                     var end: java.sql.Timestamp)