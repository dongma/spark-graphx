package org.apache.spark.util

import org.apache.spark.graphx.GraphXUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 提供spark执行的上下文，创建sparkContext及执行完成后ctx.stop
 *
 * @author Sam Ma
 * @date 2023/02/11
 */
trait LocalSparkEnv {

  /**
   * 定义spark应用上下文，传递任务函数f(SparkContext => T)及任务名称作为参数
   */
  def withSpark[T](appName: String, f: SparkContext => T): Unit = {
    val conf = new SparkConf()
    GraphXUtils.registerKryoClasses(conf)
    val sc = new SparkContext("local", appName, conf)
    try {
      f(sc)
    } finally {
      sc.stop()
    }
  }

}
