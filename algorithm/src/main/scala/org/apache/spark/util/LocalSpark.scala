package org.apache.spark.util

import org.apache.spark.graphx.GraphXUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 提供spark执行的上下文，创建sparkContext及执行完成后ctx.stop
 *
 * @author Sam Ma
 * @date 2023/02/11
 */
trait LocalSpark {

  /**
   * 定义spark应用上下文，传递任务函数f(SparkContext => T)及任务名称作为参数
   */
  def withSpark[T](appName: String, func: SparkContext => T): Unit = {
    val conf = new SparkConf()
    GraphXUtils.registerKryoClasses(conf)
    val sc = new SparkContext("local", appName, conf)
    try {
      func(sc)
    } finally {
      sc.stop()
    }
  }

  /**
   * 创建Spark session来跑任务
   */
  def withSession[T](appName: String, f: SparkSession => T): Unit = {
    val sparkBuilder = SparkSession.builder()
    // 配置spark序列化以及并行度，指定spark的master服务为local[1]，用本地部署的服务
    sparkBuilder.master("local[*]")
    sparkBuilder.config("spark.sql.shuffle.partitions", 4)
    sparkBuilder.config("spark.default.parallelism", 4)
    sparkBuilder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkBuilder.appName(s"${appName}-${this.getClass.getSimpleName}")

    val session = sparkBuilder.getOrCreate()
    try {
      f(session)
    } finally {
      session.stop()
    }
  }

}
