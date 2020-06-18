package org.apache.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{BooleanType, DataType, StructField, StructType}

/**
 * @author Sam Ma
 * @date 2020/06/18
 * 继承实现UserDefinedAggregateFunction基类，在spark中实现User Defined Function
 */
class BoolAnd extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(StructField("value", BooleanType) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("result", BooleanType) :: Nil)

  override def dataType: DataType = BooleanType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = true
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Boolean](0) && input.getAs[Boolean](0)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Boolean](0) && buffer2.getAs[Boolean](0)
  }

  override def evaluate(buffer: Row): Any = {
    buffer(0)
  }

}
