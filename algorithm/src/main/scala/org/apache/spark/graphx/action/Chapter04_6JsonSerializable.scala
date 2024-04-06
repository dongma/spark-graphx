package org.apache.spark.graphx.action

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

object Chapter04_6JsonSerializable {

  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext(new SparkConf().setMaster("local")
      .setAppName("JsonSerialize"))
    // 通过sparkContext构建图中的顶点
    val vertices = sparkContext.makeRDD(Array((1L, "Ann"), (2L, "Bill"), (3L, "Charles"),
      (4L, "Diane"), (5L, "Went to Gym this Morning")))
    // 通过sparkContext构建图中的所有边
    val edgeList = sparkContext.makeRDD(Array(Edge(1L, 2L, "is-friends-with"), Edge(2L, 3L, "is-friends-with"),
      Edge(3L, 4L, "is-friends-with"), Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))
    val myGraph = Graph(vertices, edgeList)

    /**
      * serialize graph vertices and edges to json file
      */
    myGraph.vertices.map(vertice => {
      // 使用Jackson工具进行序列化Graph对象
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)

      // 使用Jackson将Vertice对象进行序列化
      val writer = new java.io.StringWriter()
      mapper.writeValue(writer, vertice)
      writer.toString
    }).coalesce(1, true).saveAsTextFile("myGraphVerticesJSon")


    // mapPartitions() 使用该方法在某些特定场景下序列换的效率会比较高(optional)
    /*    myGraph.vertices.mapPartitions(vertice => {
          // 使用Jackson工具进行序列化Graph对象
          val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
          mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)

          // 使用Jackson将Vertices对象进行序列化
          val writer = new java.io.StringWriter()
          vertice.map(v => {
            writer.getBuffer.setLength(0)
            mapper.writeValue(writer, v)
            writer.toString
          })
        }).coalesce(1, true).saveAsTextFile("myGraphVerticesJSon")*/

    // 对Graph中的边edgeList也进行序列化
    myGraph.edges.mapPartitions(edges => {
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val writer = new java.io.StringWriter()
      edges.map(e => {
        writer.getBuffer.setLength(0)
        mapper.writeValue(writer, e)
        writer.toString
      })
    }).coalesce(1, true).saveAsTextFile("myGraphEdgesJSon")

    // 使用Jackson反序列化从json文件中构造Graph对象
    val jsonGraph = Graph(
      /**
        * Todo:
        * Exception in thread "main" java.lang.IllegalAccessError: tried to access method com.google.common.base.Stopwatch.<init>()V
        * from class org.apache.hadoop.mapred.FileInputFormat
        */
      sparkContext.textFile("myGraphVerticesJSon").mapPartitions(vertices => {
        val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        vertices.map(v => {
          val r = mapper.readValue[(Integer, String)](v, new TypeReference[(Integer, String)] {})
          (r._1.toLong, r._2)
        })
      }),
      sparkContext.textFile("myGraphEdgesJSon").mapPartitions(edges => {
        val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        edges.map(e => mapper.readValue[Edge[String]](e, new TypeReference[Edge[String]] {}))
      })
    )

    println(jsonGraph)

  }
}
