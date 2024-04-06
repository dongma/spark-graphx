package org.apache.spark.graphx.ssp

import org.apache.spark.SparkContext
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Edge, Graph, VertexId}

/**
 * @author Sam Ma
 * @date 2020/09/08
 * 使用pregel实现最短路径算法，sc为SparkContext上下文对象
 */
class ShortestPathProblem(sc: SparkContext) {

  /**
   * 定义GenericGraph对象，Graph中vertex属性可为任意类型，Edge边类型为Double
   */
  type GenericGraph[VT] = Graph[VT, Double]

  /**
   * 根据指定sourceId在图中查找最短路径
   *
   * @param graph
   * @param sourceId
   * @tparam VT
   * @return
   */
  def shortestPath[VT](graph: GenericGraph[VT], sourceId: VertexId): String = {
    val initialGraph = graph.mapVertices((id, _) =>
      if (id == sourceId) 0.0 else Double.PositiveInfinity)

    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      // vertex消息的处理函数，用于根据vertexId更新属性值
      (_, dist, newDist) => math.min(dist, newDist),
      // sendMsg函数，用于判断该向哪些vertex发消息，使用Iterator((vertexId, msg))迭代器表示
      triplet => {
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      // msgMerge的函数，当vertex收到来自不同source vertex发来的msg时 选取最小的记录
      (a, b) => math.min(a, b)
    )
    // 将graph中的消息按key进行排序，并转换为字符串用于在console打印
    sssp.vertices.sortByKey(ascending = true).collect.mkString("\n")
  }

  /**
   * 使用测试数据构建GenericGraph图对象
   *
   * @return
   */
  def testGraph: GenericGraph[String] = {
    val vertices = sc.parallelize(Array(
      (1L, "one"), (2L, "two"), (3L, "three"), (4L, "four")
    ))

    val relationships = sc.parallelize(Array(
      Edge(1L, 2L, 1.0), Edge(1L, 4L, 2.0), Edge(2L, 4L, 3.0), Edge(3L, 1L, 1.0),
      Edge(3L, 4L, 5.0)
    ))
    Graph(vertices, relationships)
  }

  /**
   * 生成一个graph图对象，其顶点的out degree是log normal graph
   *
   * @return
   */
  def randomGraph: GenericGraph[Long] =
    GraphGenerators.logNormalGraph(sc, numVertices = 6).mapEdges(e => e.attr.toDouble)

  /**
   * 将graph图转换为string类型
   *
   * @param graph
   * @tparam T
   * @return
   */
  def graphToString[T](graph: GenericGraph[T]): String = {
    import scala.compat.Platform.EOL
    s"Vertices:$EOL" + graph.vertices.collect.mkString(s"$EOL") +
      s"$EOL Edges:$EOL" + graph.edges.collect.mkString(s"$EOL")
  }

}
