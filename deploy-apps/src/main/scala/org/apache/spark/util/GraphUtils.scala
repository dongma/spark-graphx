package org.apache.spark.util

import org.apache.spark.graphx._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
 * 执行数据计算的部分，将公共计算部分抽取出来
 *
 * @author Sam Ma
 * @date 2023/04/05
 */
object GraphUtils {

  def point2pointRight(vertexDf: DataFrame, investDf: DataFrame, maxIter: Int,
                       threshold: Double): DataFrame = {
    import investDf.sparkSession.implicits._

    def mergeMaps1(a: Map[VertexId, Double], b: Map[VertexId, Double]) = {
      (a.keySet ++ b.keySet).map { k => (k, a.getOrElse(k, 0.0) + b.getOrElse(k, 0.0)) }.toMap
    }

    def mergeMsg2(a: Map[VertexId, Double], b: Map[VertexId, Double]) = {
      (a.keySet ++ b.keySet).map { k => (k, math.max(a.getOrElse(k, Double.MinValue), b.getOrElse(k, Double.MinValue))) }.toMap
    }

    // Warning: 以vertexId作为顶点时，会有数据问题导致报空指针，从顶点Vertex和关系边Edge中构建EdgeRDD
    /*val verticesRDD = vertexDf.select(col("id")).rdd.map {row =>
      val vid = row.getAs[Long]("id")
      (vid, Map(vid -> 1.0))
    }*/

    val verticesRDD = investDf.select(col("from_id").as("id"))
      .union(investDf.select(col("to_id").as("id"))).distinct().rdd.map {row =>
      val vid = row.getAs[Long]("id")
      (vid, Map(vid -> 1.0))
    }

    val edgeRDD = investDf.rdd.map {row =>
      val src = row.getAs[Long]("from_id")
      val dst = row.getAs[Long]("to_id")
      val prop = row.getAs[Double]("rate")
      Edge(src, dst, prop)
    }
    var g2 = Graph(verticesRDD, edgeRDD).partitionBy(PartitionStrategy.RandomVertexCut)

    var condition = true
    for (iter <- 1 to maxIter if condition) {
      g2.persist()

      val newVertices = g2.aggregateMessages[Map[VertexId, Double]](
        triplet => {
          val filteredSrc = triplet.srcAttr.filter(t => t._2 > threshold && t._1 != triplet.dstId)
          val srcPlus = filteredSrc.map { case (vid, prop) => if (prop > 0.3) (vid, triplet.attr) else (vid, prop * triplet.attr) }
          if (srcPlus.nonEmpty) {
            triplet.sendToDst(srcPlus)
          }
        },
        (a, b) => mergeMaps1(a, b),
        TripletFields.Src
      ).persist()

      condition = newVertices.count() > 0

      val newGraph = g2.outerJoinVertices(newVertices) { (_, oldAttr, opt) =>
        val oldMap = oldAttr
        val newMap = mergeMsg2(oldMap, opt.getOrElse(Map[VertexId, Double]()))
        (newMap, oldMap.size != newMap.size)
      }.subgraph(epred = triplet => triplet.dstAttr._2).mapVertices{ case (_, attr) => attr._1 }

      newVertices.unpersist()
      g2.unpersist()
      g2 = newGraph
    }

    g2.mapVertices { case (vid, attr) => attr.-(vid) }
      .vertices.filter(_._2.nonEmpty).flatMapValues(attr => attr)
      .map(t => (t._2._1, t._1, t._2._2)).toDF("from_id", "to_id", "prop")
  }

}
