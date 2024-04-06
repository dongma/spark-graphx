package org.apache.spark.graphx.action

import org.apache.spark.graphx.{Edge, EdgeContext, Graph}
import org.apache.spark.{SparkConf, SparkContext}

object Chapter04_3FurtherVertex {

  /**
    * sendMessage method will regard as aggregateMessag params
    * Remember this function will be called for each edge in the graph,
    * Here it simply passes on incremented counter.
    * @param context
    */
  def sendMessage(context: EdgeContext[Int, String, Int]): Unit = {
    context.sendToDst(context.srcAttr + 1);
  }

  /**
    * mergeMessage method will be called repeated for all messages delivered to a vertex,
    * the end result is the vertex will contain the highest value, or distance or over all message
    * @param a
    * @param b
    */
  def mergeMessage(a: Int, b: Int): Int = {
    math.max(a, b)
  }


  def propagateEdgeCount(graph: Graph[Int, String]): Graph[Int, String] = {
    // Generate new set of vertices
    val vertices = graph.aggregateMessages[Int](sendMessage, mergeMessage)
    val newGraph = Graph(vertices, graph.edges)

    /**
      * whether the updated graph has any new information by joining two sets of vertices
      * together, this results in Tuple2[old vertex data, new vertex data]
      */
    val check = newGraph.vertices.join(graph.vertices)
      // look at each element of the joined sets of vertices and calculate difference between them,
      // if there is no difference it will zero
      .map(x => x._2._1 - x._2._2)
      // Add up all difference, if all the vertices are the same, the total is zero
      .reduce(_ + _)
    if (check > 0)
      propagateEdgeCount(newGraph)
    else
      graph
  }

  /**
    * Todo:
    * Exception in thread "main" org.apache.spark.SparkException: Task not serializable
	  *   at org.apache.spark.util.ClosureCleaner$.ensureSerializable(ClosureCleaner.scala:403)
	  *   at org.apache.spark.util.ClosureCleaner$.clean(ClosureCleaner.scala:393)
    */
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

    val initialGraph = myGraph.mapVertices((_, _) => 0)
    val edgeCountArray = propagateEdgeCount(initialGraph).vertices.collect()
    // 向console打印出graph元组信息(Tuple)
    edgeCountArray.foreach(print(_))
  }
}
