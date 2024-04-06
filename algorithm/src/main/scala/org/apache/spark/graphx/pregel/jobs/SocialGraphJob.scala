package org.apache.spark.graphx.pregel.jobs

import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.social.{InputDataFlow, SocialGraph, USER_NAMES_TSV}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author Sam Ma
 * @date 2020/09/03
 * 创建社交媒体SocialGraphJob对象，用于离线社交关系计算
 */
object SocialGraphJob {

  private[this] val logger: Logger = LoggerFactory.getLogger(SocialGraphJob.getClass)

  def main(args: Array[String]): Unit = {
    /*
     * start up the sparkSession along with explicitly setting a given config
     */
    val spark = SparkSession.builder().appName("Spark Social graph Job")
      .config("spark.sql.warehouse.dir", "/Users/madong/datahub-repository/spark-graphx/spark-warehouse")
      .config("spark.master", "local")
      .getOrCreate()
    // 通过sparkContext创建apache graphx的图对象
    val socialGraph = new SocialGraph(spark.sparkContext)

    /*
     * 连通性算法degree.join(vertex)结果集合为0，排查原因为vertexRdd未加载到数据 (原因为：解析方法少写else)
     */
/*    val userNameCount: Long = spark.sparkContext.textFile(USER_NAMES_TSV).count()
    logger.info(s"userName Rdd count: $userNameCount")
    spark.sparkContext.textFile(USER_NAMES_TSV).take(10)
      .map(record => {
        val fields: Array[String] = record.split('\t')
        (record, fields.length)
      })
      .foreach(record => logger.info(s"raw text: ${record._1}, length: ${record._2}"))*/

    // 20/09/19 08:48:35 INFO SocialGraphJob$: vertexId: 4551, degree: 10, vertexId: 667, degree: 10, vertexId: 5618, degree: 19
//    socialGraph.graph.degrees.take(3).foreach(degree => logger.info(s"vertexId: ${degree._1}, degree: ${degree._2}"))

    logger.info(s"vertex rdd count: ${socialGraph.vertexs.count()} , edge rdd count: ${socialGraph.edges.count()}")
//    socialGraph.vertexs.take(3).foreach(vertex => logger.info(s"vertexId: ${vertex._1}, value: ${vertex._2}"))

    /*val degreeJoinRdd : RDD[(VertexId, (Int, String))] = socialGraph.graph.degrees.join(socialGraph.vertexs)
    logger.info(s"degreeJoinRdd value: ${degreeJoinRdd.count()}")
    joinValues.foreach(joinValue => logger.info(s"vertexId: ${joinValue._1}, " +
      s"degrees: ${joinValue._2._1} vertexName: ${joinValue._2._2}"))*/

    logger.info("Top 10 most-connected users: ")
    socialGraph.getMostConnectedUsers(10).foreach(rankUser => logger.info(s"**most-connected user: $rankUser"))

    logger.info("graphx connected component: ")
    val connectedUsers = socialGraph.connectedGroupByUsers
      .sortBy({ case (_, lowerVertexId) => lowerVertexId }, ascending = false)
      .take(10)
    logger.info(s"socialGraph connected group by users length: ${connectedUsers.length}")

    val triangleCount = socialGraph.socialGraphTriangleCount
    logger.info(s"triangle count in socialgraph is : $triangleCount")
  }

}
