package org.apache.spark.pregel


import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LocalSpark
import org.slf4j.LoggerFactory

/**
 * Graphx中的算法包：最短路径、联通算法、强连通内容跑下，lib包中是基于hip跳数计算的
 *
 * @author madong
 * @date 2023/07/14
 */
class ShortestPath extends Serializable {

  /** scala中type语法，类似于类型别名 */
  type VexAttr = (String, String)

  private def makeMap(x: (VertexId, Int)*) = Map(x: _*)

  /** 计算消息内容 */
  private def incrementMap(edge: EdgeTriplet[Map[VertexId, Int], _]): Map[VertexId, Int] = {
    val distance = edge.attr.asInstanceOf[Int]
    edge.dstAttr.map { case (v, d) => v -> (distance + d) }
  }

  /**
   * 一个点收到多个msg时，做消息的合并
   *
   * @return
   */
  private def mergeMsg(spmap1: SPMap, spmap2: SPMap): Map[VertexId, Int] = {
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
    }.toMap
  }

  /**
   * 生成图对象，graphx支持多种构造图的方式
   *
   * @return
   */
  private[this] def generateGraph(session: SparkSession): Graph[VexAttr, Int] = {
    val ctx: SparkContext = session.sparkContext

    val f1Races: RDD[(VertexId, VexAttr)] = ctx.parallelize(Seq((1L, ("ShangHai", "April 3")),
      (2L, ("Australia", "March 17")), (3L, ("Japan", "Feb 18")), (4L, ("Imola", "Jane 6")),
      (5L, ("France", "August 13"))))

    val airplanes: RDD[Edge[Int]] = ctx.parallelize(Seq(Edge(1L, 2L, 201), Edge(2L, 3L, 103),
      Edge(3L, 4L, 68), Edge(1L, 5L, 125), Edge(5L, 4L, 89)))
    Graph(f1Races, airplanes, defaultVertexAttr = ("Unknown", "Tomorrow"))
  }

  /**
   * 调用lib.shortestPath算法，看下效果
   *
   * @return
   */
  def shortestPaths(session: SparkSession, landMarks: Seq[VertexId]): Graph[Map[VertexId, Int], Int] = {
    val graph: Graph[VexAttr, Int] = generateGraph(session)

    val spGraph = graph.mapVertices { (vid, attr) =>
      if (landMarks.contains(vid)) makeMap(vid -> 0) else makeMap()
    }
    val initialMsg = makeMap()

    /** 接收到消息后，顶点的处理逻辑 */
    def vprogfunc(id: VertexId, attr: SPMap, msg: SPMap): Map[VertexId, Int] = {
      mergeMsg(attr, msg)
    }

    /** vertex顶点发消息的逻辑，按shortestPath的逻辑：是从dstId向srcId发送消息的 */
    def sendMsg(edge: EdgeTriplet[Map[VertexId, Int], _]): Iterator[(VertexId, Map[VertexId, Int])] = {
      val newAttr = incrementMap(edge)
      if (edge.srcAttr != mergeMsg(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
      else Iterator.empty
    }

    Pregel(spGraph, initialMsg)(vprog = vprogfunc, sendMsg, mergeMsg)
  }

}

object ShortestPath extends LocalSpark {
  val logger = LoggerFactory.getLogger(getClass)

  /** object伴生对象，创建对象调用apply方法，如：Algorithm(val1) 不需要new关键字 */
  def apply() = new ShortestPath

  def main(args: Array[String]): Unit = {
    val algolib: ShortestPath = ShortestPath()

    /* => ShortestPaths.run默认版本，每经过一条边，距离长度就会加1（基于跳数来计算）
    23/07/14 16:13:09 INFO Algorithm$: shortestPath algorithm, vertexId: 2, SpMap: Map(4 -> 2)
    23/07/14 16:13:09 INFO Algorithm$: shortestPath algorithm, vertexId: 4, SpMap: Map(4 -> 0)
      23/07/14 16:13:09 INFO Algorithm$: shortestPath algorithm, vertexId: 1, SpMap: Map(4 -> 2)
    23/07/14 16:13:09 INFO Algorithm$: shortestPath algorithm, vertexId: 5, SpMap: Map(4 -> 1)
    [rdd_52_1]
    23/07/14 16:13:09 INFO Algorithm$: shortestPath algorithm, vertexId: 3, SpMap: Map(4 -> 1)*/

    withSession("shortestPath", { session: SparkSession =>
      val result = algolib.shortestPaths(session, landMarks = Seq(4L))
      result.vertices.foreach(elem => {
        logger.info(s"shortestPath algorithm, vertexId: ${elem._1}, SpMap: ${elem._2}")
      })
    })

  }

}
