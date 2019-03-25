package org.apache.spark.buildinalgos

import org.apache.spark.graphx.{Edge, Graph, GraphLoader, PartitionStrategy}
import org.apache.spark.{SparkConf, SparkContext}

object Chapter05_2TriangleCount {

  def main(args: Array[String]): Unit = {
//    C:\Users\ChinaDaas\Downloads\soc-Slashdot0811\Slashdot0811.txt

    val sparkContext = new SparkContext(new SparkConf().setMaster("local")
      .setAppName("pageRank"))
    val graph = GraphLoader.edgeListFile(sparkContext, "C:/Users/ChinaDaas/Downloads/soc-Slashdot0811/Slashdot0811.txt").cache()
    val filterGraph = Graph(graph.vertices, graph.edges.map(edge =>
        if (edge.srcId < edge.dstId) edge
        else new Edge(edge.dstId, edge.srcId, edge.attr)
      )
    ).partitionBy(PartitionStrategy.RandomVertexCut)

    // triangleCount()实际上用于计算Triangle的数量,return Graph[Int, ED]
    val result = (0 to 6).map(i => filterGraph.subgraph(vpred = (vid, _) => vid >= i * 10000 && vid < (i + 1) * 10000)
      .triangleCount.vertices.map(_._2)
      .reduce(_ + _))
    println(result)

  }
}
