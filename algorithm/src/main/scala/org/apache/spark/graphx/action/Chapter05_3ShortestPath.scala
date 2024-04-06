package org.apache.spark.graphx.action

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

object Chapter05_3ShortestPath {

  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext(new SparkConf().setMaster("local")
      .setAppName("spark"))
    // 通过sparkContext构建图中的顶点
    val vertices = sparkContext.makeRDD(Array((1L, "Ann"), (2L, "Bill"), (3L, "Charles"),
      (4L, "Diane"), (5L, "Went to Gym this Morning")))
    // 通过sparkContext构建图中的所有边
    val edgeList = sparkContext.makeRDD(Array(Edge(1L, 2L, "is-friends-with"), Edge(2L, 3L, "is-friends-with"),
      Edge(3L, 4L, "is-friends-with"), Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))
    val myGraph = Graph(vertices, edgeList)

//    ShortestPaths.run(myGraph, Array(3)).vertices.collect()
//    println(result)
  }
}
