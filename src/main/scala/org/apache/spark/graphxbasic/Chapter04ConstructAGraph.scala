package org.apache.spark.graphxbasic

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

object Chapter04ConstructAGraph {

  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext(new SparkConf().setMaster("local")
      .setAppName("hello world"))
    // 通过sparkContext构建图中的顶点
    val vertices = sparkContext.makeRDD(Array((1L, "Ann"), (2L, "Bill"), (3L, "Charles"),
      (4L, "Diane"), (5L, "Went to Gym this Morning")))
    // 通过sparkContext构建图中的所有边
    val edgeList = sparkContext.makeRDD(Array(Edge(1L, 2L, "is-friends-with"), Edge(2L, 3L, "is-friends-with"),
      Edge(3L, 4L, "is-friends-with"), Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))
    val myGraph = Graph(vertices, edgeList)
    // 获取构建的Graph对象中所有的顶点的集合,并将其打印到控制台上
    val verticesArray = myGraph.vertices.collect
    // console result: (4,Diane)(1,Ann)(3,Charles)(5,Went to Gym this Morning)(2,Bill)
    verticesArray.foreach(print(_))

    // 使用triplets方法打印出点和边的组合,console: ((1,Ann),(2,Bill),is-friends-with)((2,Bill),(3,Charles),is-friends-with)((3,Charles),
    // (4,Diane),is-friends-with)((3,Charles),(5,Went to Gym this Morning),Wrote-status)((4,Diane),(5,Went to Gym this Morning),Likes-status)
    myGraph.triplets.foreach(print(_))

  }
}
