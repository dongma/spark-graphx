package org.apache.spark.helper

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField}

/**
 * csv中字段的映射StructField类型，用于在spark中注册df
 *
 * @author Sam Ma
 * @date 2023/04/05
 */
object FieldMapping {

  /** 映射字段，按字段名称将其映射为StructField */
  def getStructField: String => StructField = {
    case field@"id" => StructField(field, StringType)
    case field@"keyNo" => StructField(field, LongType)
    case field@"name" => StructField(field, StringType)
    case field@"type" => StructField(field, StringType)
    case field@"status" => StructField(field, StringType)
    case field@"label" => StructField(field, StringType)

    // 关系边csv上的属性，from、to在RDD处理时要替换为Long类型
    case field@"from" => StructField(field, StringType)
    case field@"to" => StructField(field, StringType)
    case field@"rate" => StructField(field, IntegerType)
    case field@"eid" => StructField(field, LongType)
    case field@"holder" => StructField(field, IntegerType)
    // 最后一个默认项default项，匹配String类型
    case field => StructField(field, StringType)
  }

  /**
   * 依赖csv文件的header表头
   */
  val COMPANY_FIELDS: String = "id,keyNo,name,status,label"
  val PERSON_FIELDS: String = "id,md5,name,type,keyNo"

  val PINVEST_FIELDS: String = "id,type,from,to,rate,eid"
  val CINVEST_FIELDS: String = "id,type,from,to,holder,eid"

  /** 根据csv文件名称，获取其对应的header */
  def getSchema: String => String = {
    case name@"person" => PERSON_FIELDS
    case name@"company" => COMPANY_FIELDS
    case name@"pinvest_rel" => PINVEST_FIELDS
    case name@"cinvest_rel" => CINVEST_FIELDS
  }

}
