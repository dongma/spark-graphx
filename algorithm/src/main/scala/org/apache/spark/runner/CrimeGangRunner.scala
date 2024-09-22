package org.apache.spark.runner

import org.apache.spark.graphx.{Graph, PartitionStrategy, TripletFields, VertexId}
import org.apache.spark.helper.CsvDfHelper
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LocalSpark
import org.slf4j.LoggerFactory

/**
 * 犯罪团伙分析，3人3案件
 * Created by madong on 2024/9/19.
 */
class CrimeGangRunner {

  val logger = LoggerFactory.getLogger(getClass)

  /** 利用点和关系边的csv构建图 */
  def generateGraph(spark: SparkSession) = {
    // 1、构建实体点
    val ajDS = CsvDfHelper.getVertexRddFromCsv(spark, "example-data/ts_aj/ts_aj.csv")
    val suspectDS = CsvDfHelper.getVertexRddFromCsv(spark, "example-data/ts_aj/ts_suspect.csv")
//    val addrDS = CsvDfHelper.getVertexRddFromCsv(spark, "example-data/ts_aj/ts_addr.csv")
    val vertexRdd = ajDS.union(suspectDS).rdd

    // 2、构建关系边
    val ajsus = CsvDfHelper.getEdgeRddFromCsv(spark, "example-data/ts_aj/ts_link_aj_suspect.csv")
//    val ajaddr = CsvDfHelper.getEdgeRddFromCsv(spark, "example-data/ts_aj/ts_link_aj_addr.csv")
//    val edgeRdd = ajsus.union(ajaddr).rdd

    Graph(vertexRdd, ajsus.rdd)
      .partitionBy(PartitionStrategy.RandomVertexCut)
  }

  /** 从实体关系中，寻找满足特征的犯罪团体 */
  def getCrimeGroup(graph: Graph[Map[Long, Set[Long]], String]) = {

    def mergeMaps1(msg1: Map[VertexId, Set[Long]], msg2: Map[VertexId, Set[Long]]) = {
      (msg1.keySet ++ msg2.keySet).map { key => (key, msg1.getOrElse(key, Set[Long]()).++(msg2.getOrElse(key, Set[Long]()))) }.toMap
    }
    /* 定义消息体msgBody格式：
     {
       案件id1->[涉案人id1, 涉案人id2, 涉案人id3],
       案件id2->[涉案人id1, 涉案人id4, 涉案人id2, 涉案人id5]
     }  */
    val newVertices = graph.aggregateMessages[Map[VertexId, Set[Long]]](
      triplet => {
        // 定义谁发消息，考虑了下，整体流程：案件--发msg=>> 嫌疑人, msg格式为: {案件id, Set(嫌疑人id)}
        val msg = Map(triplet.srcId -> Set(triplet.dstId))
        triplet.sendToSrc(msg)
      },
      (msg1, msg2) => mergeMaps1(msg1, msg2),
      TripletFields.All
    ).persist()

    logger.info(s"==== getCrimeGroup ==> 输出msg merged后的节点, active count: ${newVertices.count()}")
    /* case1,3人3案件模型，迭代第1轮次，sendToDst (嫌疑人)，消息merge后，活跃的vertices如下:
    22:24:59.899 [main] INFO org.apache.spark.scheduler.DAGScheduler - Job 10 finished: take at CrimeGangRunner.scala:56, took 0.020007 s
    (12110126,Map(12900991 -> Set(12110126)))
    (10920128,Map(12900991 -> Set(10920128)))
    (10290311,Map(10290102 -> Set(10290311), 10290291 -> Set(10290311)))
    (10290111,Map(10290102 -> Set(10290111)))
    (10290921,Map(10290102 -> Set(10290921), 10290291 -> Set(10290921), 12900992 -> Set(10290921)))
    (10290101,Map(10290102 -> Set(10290101), 10290291 -> Set(10290101), 10920128 -> Set(10290101), 12900992 -> Set(10290101)))
    (12110117,Map(10290102 -> Set(12110117), 12900991 -> Set(12110117)))
    (10910129,Map(12900991 -> Set(10910129)))
    (20192301,Map(93012001 -> Set(20192301)))
    (10290221,Map(10290102 -> Set(10290221), 10290291 -> Set(10290221), 12900992 -> Set(10290221)))
    (20192001,Map(93012001 -> Set(20192001)))
    (13019201,Map(12900992 -> Set(13019201)))
    (20192101,Map(10920128 -> Set(20192101), 93012001 -> Set(20192101))) */

    /* case2, 3人3案件模型，迭代第1轮次，sendToSrc (案件)，消息merge后，活跃的vertices如下:
    (12900992,Map(12900992 -> Set(10290101, 10290221, 10290921, 13019201)))
    (10290102,Map(10290102 -> HashSet(10290111, 10290221, 10290921, 10290311, 12110117, 10290101)))
    (10920128,Map(10920128 -> Set(10290101, 20192101)))
    (12900991,Map(12900991 -> Set(10910129, 10920128, 12110117, 12110126)))
    (10290291,Map(10290291 -> Set(10290101, 10290221, 10290311, 10290921)))
    (93012001,Map(93012001 -> Set(20192001, 20192101, 20192301))) */

    /* case3, 3人3案件模型, 迭代第1轮次, sendToDst和sendToSrc同时发送时，活跃的点是 case1与case2的总和
     */
    newVertices.take(20).foreach(println)

  }
}

object CrimeGangRunner extends LocalSpark {

  def instance(): CrimeGangRunner = {
    new CrimeGangRunner()
  }

  def main(args: Array[String]): Unit = {
    withSession("CrimeGang Analysis", { spark: SparkSession =>
      val instance = CrimeGangRunner.instance()
      val graph = instance.generateGraph(spark)
      instance.getCrimeGroup(graph)
    })
  }

}
