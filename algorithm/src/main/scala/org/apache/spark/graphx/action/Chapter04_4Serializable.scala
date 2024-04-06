package org.apache.spark.graphx.action

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext}

object Chapter04_4Serializable {

  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext(new SparkConf().setMaster("local")
      .setAppName("spark-graph"))
    // 通过sparkContext构建图中的顶点
    val vertices = sparkContext.makeRDD(Array((1L, "Ann"), (2L, "Bill"), (3L, "Charles"),
      (4L, "Diane"), (5L, "Went to Gym this Morning")))
    // 通过sparkContext构建图中的所有边
    val edgeList = sparkContext.makeRDD(Array(Edge(1L, 2L, "is-friends-with"), Edge(2L, 3L, "is-friends-with"),
      Edge(3L, 4L, "is-friends-with"), Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))
    val myGraph = Graph(vertices, edgeList)

    /**
      * write graph to a standard hadoop sequence file, which is a binary file contains sequence file.
      * The Spark RDD Api function saveAsObject() saves to Hadoop sequence file
      */
    myGraph.vertices.saveAsObjectFile("myGraphVertices")
    myGraph.edges.saveAsObjectFile("myGraphEdges")

    /**
      * option2:
      * 将Graph对象进行序列化为HDFS或者S3时,为了避免生成多个文件可以使用 coalesce(1, true)方法进行序列化
      */
//    myGraph.vertices.coalesce(1, true).saveAsObjectFile("myGraphVertices")
//    myGraph.edges.coalesce(1, true).saveAsObjectFile("myGraphEdges")

    /**
      * read graph object from binary hadoop sequence file
      */
    val deserializeGraph = Graph(sparkContext.objectFile[(VertexId, String)]("myGraphVertices"),
      sparkContext.objectFile[Edge[String]]("myGraphEdges"))
    println(deserializeGraph)

  }
}
