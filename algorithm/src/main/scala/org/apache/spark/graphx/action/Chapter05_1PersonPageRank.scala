package org.apache.spark.graphx.action

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}

object Chapter05_1PersonPageRank {

  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext(new SparkConf().setMaster("local")
      .setAppName("pageRank"))
    val graph = GraphLoader.edgeListFile(sparkContext, "D:/datahub/spark-graphx/src/main/resources/Cit-HepTh.txt")
    val graphOps = graph.personalizedPageRank(9207016, 0.001)
        .vertices
        .filter(_._1 != 9207016)
        .reduce((a, b) => if (a._2 > b._2) a else b)
    println(graphOps)
  }
}
