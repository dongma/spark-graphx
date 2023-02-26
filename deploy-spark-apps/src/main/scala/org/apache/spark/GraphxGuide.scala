package org.apache.spark

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LocalSparkEnv

/**
 * GraphX Programming Guide,展示的图计算示例
 *
 * @author Sam Ma
 * @date 2023/02/11
 */
class GraphxGuide {

  /**
   * 从RDD中构建属性图，VD类型(rxin,student), ED类型为(College)
   */
  def propertyGraph(sc: SparkContext): Graph[(String, String), String] = {
    // 以人员作为顶点创建VertexRdd，依据人之间关系创建边Rdd
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Seq((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")), (5L, ("franklin", "prof")),
        (2L, ("istoica", "prof"))))
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Seq(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"), Edge(2L, 5L, "colleague"),
        Edge(5L, 7L, "pi")))
    // 定义一个默认用户(default vertex)，当关系边缺失顶点时，使用此默认顶点
    val defaultUser = ("John Doe", "Missing")
    Graph(users, relationships, defaultUser)
  }

}

object GraphxGuide extends LocalSparkEnv with Logging {

  /** 在object伴生对象中，定义apply方法，创建对象可省略new keyword */
  def apply() = new GraphxGuide()

  def main(args: Array[String]): Unit = {
    val guideApp = GraphxGuide()

    withSpark("property graph", { sc: SparkContext =>
      val graph: Graph[(String, String), String] = guideApp.propertyGraph(sc)
      // 统计职位为"postdoc"人员数，Edge中源顶点>目的顶点 edge的数量
      val posters = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
      // Count all the edges where src > dst
      val edgeNum = graph.edges.filter { e => e.srcId > e.dstId }.count
      logInfo(s"属性图中posters数量为: ${posters}, edge中srcId大于dstId边的数量为: ${edgeNum}")

      // Graphx的Triplet属性，既包含srcId、dstId也可以包含顶点及边的属性
      val facts: RDD[String] = graph.triplets.map {
        triplet => triplet.srcAttr._1 + "# is the " + triplet.attr + " of #" + triplet.dstAttr._1
      }
      facts.collect.foreach(println(_))

      // 属性操作，给学生姓名前加上Ph.D前缀，如:"rxin" -> "Ph.D rxin"
      val newGraph = graph.mapVertices((id, attr) => {
        if (attr._2.eq("student")) {
          ("Ph.D " + attr._1, attr._1)
        } else {
          attr
        }
      })
      val student = newGraph.vertices.filter {case (id, attr) => attr._2.equals("student")}.first()
      println(s"Welcome the student [${student._2._1}] from UC Berkeley")
    })
  }

}
