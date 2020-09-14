package org.apache.spark.graphx.pregel.jobs.ssp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.ssp.ShortestPathProblem

/**
 * @author Sam Ma
 * @date 2020/09/10
 * 运行最短路径spark job任务, 自定义graph图nodes和links对象
 */
object ShortestPathProblemJob extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkContext = new SparkContext("local[*]", "ShortestPathProblemJob")
  val ssp = new ShortestPathProblem(sparkContext)

  val sourceIdForTest: VertexId = 3
  val testGraph = ssp.testGraph
  val resultOnTestGraph = ssp.shortestPath(testGraph, sourceIdForTest)
  println(s"test graph:\n ${ssp.graphToString(testGraph)}\n\n" +
    s"distance on the test graph: $resultOnTestGraph\n")

  /*
   * 使用随机生成的graph对象来find最短路径内容
   */
  val sourceIdForRandom: VertexId = 4
  val randomGraph = ssp.randomGraph
  val resultOnRandomGraph = ssp.shortestPath(randomGraph, sourceIdForRandom)
  println(s"generated randomGraph: \n ${ssp.graphToString(randomGraph)}\n\n"
    + s"distances on the random graph: $resultOnRandomGraph\n")

}
