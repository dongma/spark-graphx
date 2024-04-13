package org.apache.spark.tuning

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, PartitionID, VertexId}
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author Sam Ma
 * @date 2020/09/01
 * 构建social图对象，从tsv中抽取出来Vertex和Edge边对象
 */
class SocialGraph(sc: SparkContext) {

  private[this] val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * 自定义type类型，其中ConnectedUser、DegreeOfSeparation简化scala中元组
   */
  type ConnectedUser = (PartitionID, String)

  type DegreeOfSeparation = (Double, String)

  def vertexs: RDD[(VertexId, String)] = sc.textFile(USER_NAMES_TSV).flatMap(InputDataFlow.parseNames)

  def edges: RDD[Edge[PartitionID]] = sc.textFile(USER_GRAPH_TSV).flatMap(InputDataFlow.makeEdges)

  /**
   * 使用vertexs和edges构建graph图对象，并将构建的图cache()
   * @return                                          
   */
  def graph: Graph[String, PartitionID] = {
    val graph: Graph[String, PartitionID] = Graph(vertexs, edges).cache()
    logger.info(s"vertex size: ${graph.vertices.count()}, edge size: ${graph.edges.count()}")
    graph
  }

  /**
   * 获取在社交媒体上关系数较多的人员名称
   * @param amount
   * @return
   */
  def getMostConnectedUsers(amount: Int): Array[(VertexId, ConnectedUser)] = {
    graph.degrees.join(vertexs)
      .sortBy({ case (_, (userName, _)) => userName }, ascending = false)
      .take(amount)
  }

  /**
   * 获取graph图中的连通结点
   * @return
   */
  def connectedComponent = graph.connectedComponents().vertices

  /**
   * 通过用户名称对连通图进行分组
   * @return
   */
  def connectedGroupByUsers = {
    vertexs.join(connectedComponent).map {
      case (_, (username, comp)) => (username, comp)
    }
  }

  /**
   * 返回社交媒体图中三角形的数量
   * @return
   */
  def socialGraphTriangleCount = graph.triangleCount()

  /**
   * 使用Pregel按广度优先算法从给定点VertexId查找相关联点
   * @param root
   * @return breath-first search搜寻的graph图对象
   */
  private def getBFS(root: VertexId) = {
    val initialGraph = graph.mapVertices((id, _) =>
      if (id == root) 0.0 else Double.PositiveInfinity)

    /* initialGraph.pregel()函数参数: vprog: (VertexId, VD, A) => VD, sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
     *    mergeMsg: (A, A) => A)
     * vprog: 当前vertex收到msg消息如何进行更新结点属性
     * sendMsg: 该函数返回Iterator表示下一轮将要发送消息的vertex列表
     * mergeMsg: 用于合并从多个结点发送来的msg消息，目前是比较获取最小的msg value值
     */
    val bfsGraph = initialGraph.pregel(Double.PositiveInfinity, maxIterations = 10)(
      (_, attr, msg) => math.min(attr, msg),
      triplet => {
        if (triplet.srcAttr != Double.PositiveInfinity) {
          Iterator((triplet.dstId, triplet.srcAttr + 1))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b)).cache()
    bfsGraph
  }

  /**
   * 单个用户的degree作为adapter适配器去调用getBFS()方法
   * @param root
   * @return
   */
  def degreeOfSeparationSingleUser(root: VertexId): Array[(VertexId, DegreeOfSeparation)] = {
    getBFS(root).vertices.join(vertexs).take(100)
  }

  def degreeOfSeparationTwoUser(firstUser: VertexId, secondUser: VertexId) = {
    getBFS(firstUser)
      .vertices
      .filter { case (vertexId, _) => vertexId == firstUser }
      .collect map { case (_, degree) => degree }
  }

}
