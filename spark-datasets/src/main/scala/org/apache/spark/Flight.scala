package org.apache.spark

/**
 * @author Sam Ma
 * @date 2020/06/21
 * 定义封装csv和json中summary类型数据记录
 */
/*
 * Flight映射scala中原始数据内容，将csv或json文件内容转化为实体对象
 */
case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)

case class FlightWithMetadata(count: BigInt, randomData: BigInt)