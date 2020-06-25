package org.apache.spark

/**
 * @author Sam Ma
 * @date 2020/06/25
 * 自定义Partitioner分区，and only if the key matches a certain format
 */
class DomainPartitioner extends Partitioner {

  override def numPartitions: Int = 3

  override def getPartition(key: Any): Int = {
    val customerId = key.asInstanceOf[Double].toInt
    if (customerId == 17850.0 || customerId == 12583.0) {
      return 0
    } else {
      return new java.util.Random().nextInt(2) + 1
    }
  }

}
