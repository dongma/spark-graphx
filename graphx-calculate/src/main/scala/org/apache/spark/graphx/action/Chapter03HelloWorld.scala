package org.apache.spark.graphx.action

import org.apache.spark.{SparkConf, SparkContext}

object Chapter03HelloWorld {

  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext(new SparkConf().setMaster("local")
        .setAppName("hello world"));
    // 通过context对象创建RDD
    val rdd = sparkContext.makeRDD(Array("Hello", "World"));
    // 打印出RDD的内容到console
    rdd.foreach(println(_))
    sparkContext.stop
  }

}
