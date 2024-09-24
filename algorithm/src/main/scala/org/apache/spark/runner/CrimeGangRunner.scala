package org.apache.spark.runner

import org.apache.spark.graphx._
import org.apache.spark.helper.CsvDfHelper
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LocalSpark
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * 犯罪团伙分析，3人3案件
 * Created by madong on 2024/9/19.
 */
class CrimeGangRunner extends Serializable {

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

    /* 识别犯罪团伙，案件数量需大于3、并且都存在(一组大于等于3人)的团伙 */
    def detectCommunity(vertex: (VertexId, Map[VertexId, Set[Long]])) = {
      val ajNum = vertex._2.keySet.size
      if (ajNum < 3) {
        false
      } else {
        /* Map(10290102 -> HashSet(10290111, 10290221, 10290921, 10290311, 12110117, 10290101),
        10290291 -> Set(10290101, 10290221, 10290311, 10290921),
        12900992 -> Set(10290101, 10290221, 10290921, 13019201)) */
        val groupIdMap = permutationSuspects(vertex._2)
        // 重复的groupid，都来自于不同的案件
        groupIdMap.values.exists(count => count >= 3)
      }
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
    /* 3人3案件模型，迭代第1轮次，sendToSrc (案件)，消息merge后，活跃的vertices如下:
    (12900992,Map(12900992 -> Set(10290101, 10290221, 10290921, 13019201)))
    (10290102,Map(10290102 -> HashSet(10290111, 10290221, 10290921, 10290311, 12110117, 10290101)))
    (10920128,Map(10920128 -> Set(10290101, 20192101)))
    (12900991,Map(12900991 -> Set(10910129, 10920128, 12110117, 12110126)))
    (10290291,Map(10290291 -> Set(10290101, 10290221, 10290311, 10290921)))
    (93012001,Map(93012001 -> Set(20192001, 20192101, 20192301))) */

    newVertices.take(20).foreach(println)
    // Step1: 嫌疑人向案件发消息
    val newGraph = graph.outerJoinVertices(newVertices) {(_, oldAttr, opt) =>
      val newAttr = mergeMaps1(oldAttr, opt.getOrElse(Map[VertexId, Set[Long]]()))
      // 案件的嫌疑人数量 >= 3 或者 当前点为"嫌疑人"点时，继续保留
      val suspectNums = newAttr.flatMap(_._2).size
      (newAttr, suspectNums >= 3 || opt.isEmpty)
    }.subgraph(vpred = (_, vd) => vd._2).mapVertices {
      case (_, attr) => attr._1
    }
    /* 消息符合预期：
     (12110126,Map())
    (12900992,Map(12900992 -> Set(10290101, 10290221, 10290921, 13019201)))
    (10290102,Map(10290102 -> HashSet(10290111, 10290221, 10290921, 10290311, 12110117, 10290101)))
    (10290311,Map())
    (10290111,Map())
    (12900991,Map(12900991 -> Set(10910129, 10920128, 12110117, 12110126)))
     */
    logger.info(s"==== getCrimeGroup ==>, 案件消息与graph中案件实体合并后，点的信息：")
    newGraph.vertices.take(20).foreach(println)

    // Step2: 案件聚集后向嫌疑人发消息
    val activeVertices = newGraph.aggregateMessages[Map[VertexId, Set[Long]]](
      triplet => {
        // 定义谁发消息，考虑了下，整体流程：案件--发msg=>> 嫌疑人, msg格式为: {案件id, Set(嫌疑人id)}
        val msg = triplet.srcAttr
        triplet.sendToDst(msg)
      },
      (msg1, msg2) => mergeMaps1(msg1, msg2),
      TripletFields.All
    ).persist()
    logger.info(s"==== getCrimeGroup ==> 输出msg merged后的节点, 案件发消息后，active count: ${activeVertices.count()}")
    activeVertices.take(20).foreach(println)
    /*
     activate节点符合预期，部分数据：
     (12110126,Map(12900991 -> Set(10910129, 10920128, 12110117, 12110126)))
    (10290311,Map(10290102 -> HashSet(10290111, 10290221, 10290921, 10290311, 12110117, 10290101), 10290291 -> Set(10290101, 10290221, 10290311, 10290921)))
    (10290111,Map(10290102 -> HashSet(10290111, 10290221, 10290921, 10290311, 12110117, 10290101)))
    (10290921,Map(10290102 -> HashSet(10290111, 10290221, 10290921, 10290311, 12110117, 10290101), 10290291 -> Set(10290101, 10290221, 10290311, 10290921), 12900992 -> Set(10290101, 10290221, 10290921, 13019201)))
     */
    val filterGraph = newGraph.outerJoinVertices(activeVertices) {(_, oldAttr, opt) =>
      val newAttr = mergeMaps1(oldAttr, opt.getOrElse(Map[VertexId, Set[Long]]()))
      // 相关案件都会聚集到 => 嫌疑人节点上
      val ajNum = newAttr.keySet.size
      (newAttr, ajNum >= 3 || opt.isEmpty)
    }.subgraph(vpred = (_, vd) => vd._2).mapVertices {
      case (_, attr) => attr._1
    }

    logger.info("==== getCrimeGroup ==>, 嫌疑人消息与graph中案件实体合并后，点的信息：")
    filterGraph.vertices.take(20).foreach(println)
    /* (10290102,Map(10290102 -> HashSet(10290111, 10290221, 10290921, 10290311, 12110117, 10290101)))
    (10290311,Map(10290102 -> HashSet(10290111, 10290221, 10290921, 10290311, 12110117, 10290101), 10290291 -> Set(10290101, 10290221, 10290311, 10290921), 12900992 -> HashSet(10290221, 13019201, 10290101, 10290921, 10290311)))
    (12900991,Map(12900991 -> Set(10910129, 10920128, 12110117, 12110126)))
    (10290921,Map(10290102 -> HashSet(10290111, 10290221, 10290921, 10290311, 12110117, 10290101), 10290291 -> Set(10290101, 10290221, 10290311, 10290921), 12900992 -> HashSet(10290221, 13019201, 10290101, 10290921, 10290311)))
    (10290101,Map(10290102 -> HashSet(10290111, 10290221, 10290921, 10290311, 12110117, 10290101), 10290291 -> Set(10290101, 10290221, 10290311, 10290921), 12900992 -> HashSet(10290221, 13019201, 10290101, 10290921, 10290311)))
    (21192101,Map())
    (10290291,Map(10290291 -> Set(10290101, 10290221, 10290311, 10290921)))
    (93012001,Map(93012001 -> Set(20192001, 20192101, 20192301)))
    (10290221,Map(10290102 -> HashSet(10290111, 10290221, 10290921, 10290311, 12110117, 10290101), 10290291 -> Set(10290101, 10290221, 10290311, 10290921), 12900992 -> HashSet(10290221, 13019201, 10290101, 10290921, 10290311)))
     */
    val suspects = filterGraph.vertices.filter(vertex => detectCommunity(vertex))
    logger.info(s"==== getCrimeGroup ==>, detectCommunity检测之后，嫌疑人数：${suspects.count()}")
    suspects.take(10).foreach(println)
    suspects
  }

  /* 对案件嫌疑人进行排序分组，3个涉案人员为一组 */
  def permutationSuspects(aggVals: Map[VertexId, Set[Long]]) = {
    val groupIdMap = new mutable.HashMap[String, Long]()

    aggVals.foreach(entry => {
      val sortIds = entry._2.toSeq.sorted
      if (sortIds.size == 3) {
        // 元素刚好为3：groupId, "10290921,10290221,10290111"
        val groupId = sortIds.mkString(",")
        groupIdMap.put(groupId, 1L + groupIdMap.getOrElse(groupId, 0L))
      } else {
        // 元素大于3时，对id排序后按3个一组进行组合
        for (i <- 2 until  sortIds.length) {
          val groupId = sortIds.slice(i -2, i + 1).mkString(",")
          groupIdMap.put(groupId, 1L + groupIdMap.getOrElse(groupId, 0L))
        }
      }
    })
    groupIdMap
  }

  def formatRows(suspects: VertexRDD[Map[VertexId, Set[Long]]], spark: SparkSession) = {
    import spark.implicits._
    // 数据格式为: (涉案团伙成员 -> 共同参与案件)，样例格式为：(Seq(涉案人1,涉案人2,涉案人3) -> Seq(案件1, 案件2, 案件3))
    suspects.map(vertex => {
      val reasonMap = new mutable.HashMap[String, String]()
      val groupIds = permutationSuspects(vertex._2).filter(entry => entry._2 >= 3).keys.toSeq
      for (groupId <- groupIds) {
        val suspectIds: Seq[String] = groupId.split(",").toSeq
        // 找团伙疑似人共同参与的案件，案件数 >= 3
        var ajIds = Seq[Long]()
        vertex._2.foreach(vAttr => {
          val ajId = vAttr._1
          val matched = suspectIds.forall(pid => vAttr._2.contains(pid.toLong))
          if (matched) {
            ajIds = ajIds:+ajId
          }
        })
        reasonMap.put(groupId, ajIds.mkString(","))
      }

      val members = reasonMap.keys.flatMap(_.split(",")).toSeq.mkString("$")
      (vertex._1, members, reasonMap)
    }).toDF("suspid", "members", "reasonMap")
  }

}

object CrimeGangRunner extends LocalSpark {

  val logger = LoggerFactory.getLogger(getClass)

  def instance(): CrimeGangRunner = {
    new CrimeGangRunner()
  }

  def main(args: Array[String]): Unit = {
    withSession("CrimeGang Analysis", { spark: SparkSession =>
      val instance = CrimeGangRunner.instance()
      val graph = instance.generateGraph(spark)
      val suspectRdd = instance.getCrimeGroup(graph)
      val rowsDF = instance.formatRows(suspectRdd, spark)
      /*
      [10290311,10290221$10290311$10290921,Map(10290221,10290311,10290921 -> 10290102,10290291,12900992)]
      [10290921,10290221$10290311$10290921,Map(10290221,10290311,10290921 -> 10290102,10290291,12900992)]
      TODO ~ 10290101, 这一条需要处理，其对应的团伙不包含"本身"
      [10290101,10290221$10290311$10290921,Map(10290221,10290311,10290921 -> 10290102,10290291,12900992)]
      [10290221,10290221$10290311$10290921,Map(10290221,10290311,10290921 -> 10290102,10290291,12900992)]
       */
      logger.info(s"CrimeGangRunner runner, 图谱中3人3年3案 疑似团伙人数为: ${rowsDF.count()}")
      rowsDF.rdd.foreach(println)
    })
  }

}
