package org.apache.spark.graphx.action

import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph}
import org.apache.spark.{SparkConf, SparkContext}

object Chapter04_2TransforGraph {

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

    // 寻找所有起始顶点名称含有"a",并且边的属性包含"is-friends-with"的元组(但是并未生效)
//    val collectTuple = myGraph.mapTriplets(tuple => (tuple.attr, tuple.attr =="is-friends-with"
//      && tuple.srcAttr.toLowerCase.contains("a"))).triplets.collect
//    collectTuple.foreach(print(_))

    // 使用EdgeTriplet对边进行过滤
    val filterTuple = myGraph.mapTriplets((tuple => (tuple.attr, tuple.attr =="is-friends-with"
      && tuple.srcAttr.toLowerCase.contains("a"))): (EdgeTriplet[String, String]=> ((String, Boolean))))
      .triplets.collect()
    filterTuple.foreach(print(_))

    // 使用aggregateMessages计算图顶点的出度
    val inDegree = myGraph.aggregateMessages[Int](_.sendToDst(1), _+_).collect()
    inDegree.foreach(print(_))

    // 使用join方法使得查询结果更具有易读性
    val joinGraphInfo = myGraph.aggregateMessages[Int](_.sendToDst(1), _+_)
      .join(myGraph.vertices).collect()
    joinGraphInfo.foreach(print(_))

    // map()与swap()方法结合使用,对于不必要的输出进行清理
    val swapGraphInfo = myGraph.aggregateMessages[Int](_.sendToDst(1), _+_)
          .join(myGraph.vertices).map(_._2.swap).collect()
    swapGraphInfo.foreach(print(_))

    // getOrElse()方法对不存在出度的顶点设置出度为0
    val getOrElseGraphInfo = myGraph.aggregateMessages[Int](_.sendToDst(1), _+_)
          .rightOuterJoin(myGraph.vertices).map(x => (x._2._2, x._2._1.getOrElse(0))).collect()
    getOrElseGraphInfo.foreach(print(_))
  }
}
