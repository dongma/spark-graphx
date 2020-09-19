package org.apache.spark.graphx.social

import org.apache.spark.graphx.{Edge, VertexId}

import scala.collection.mutable.ListBuffer

/**
 * @author Sam Ma
 * @date 2020/09/01
 * InputDataFlow对象用于从tsv中解析Vertex点和Edge边对象
 */
object InputDataFlow {

  /**
   * 将userNames.tsv文件记录5	"Frank"解析为(VertexId, String)格式
   * @param line
   * @return
   */
  def parseNames(line: String): Option[(VertexId, String)] = {
    val fields = line.split('\t')
    if (fields.length > 1) {
      Some(fields(0).trim.toLong, fields(1))
    } else {
      None
    }
  }

  /**
   * 将userGraph.tsv中的记录转换为关系边Edge
   * @param line
   * @return
   */
  def makeEdges(line: String): List[Edge[Int]] = {
    var edges = new ListBuffer[Edge[Int]]()
    val fields = line.split(" ")
    // userGraph.tsv文件一行记录中第一个元素表示起始点的id, 其余的id为终止点的id, 可用此关系构建Edge边
    val originId = fields(0)
    (1 until fields.length)
      .foreach(destId => edges += Edge(originId.toLong, destId.toLong, 0))
    edges.toList
  }

}
