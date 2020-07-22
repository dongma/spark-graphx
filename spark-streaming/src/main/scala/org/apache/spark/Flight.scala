package org.apache.spark

/**
 * @author Sam Ma
 * @date 2020/07/21
 * 使用DataSet处理数据流需将进行转换，可进行数据类型检查
 */
case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)
