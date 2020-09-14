package org.apache.spark.graphx.pregel.jobs

import org.apache.spark.graphx.social.SocialGraph
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

    logger.info("Top 10 most-connected users: ")
    val rankUsers = socialGraph.getMostConnectedUsers(10)
    logger.info(s"all ranked users length: ${rankUsers.length}")
    //    socialGraph.getMostConnectedUsers(10).foreach(rankUser => logger.info(s"**most-connected user: $rankUser"))

    logger.info("graphx connected component: ")
    val connectedUsers = socialGraph.connectedGroupByUsers
      .sortBy({ case (_, lowerVertexId) => lowerVertexId }, ascending = false)
      .take(10)
    logger.info(s"socialGraph connected group by users length: ${connectedUsers.length}")

    val triangleCount = socialGraph.socialGraphTriangleCount
    logger.info(s"triangle count in socialgraph is : $triangleCount")
  }

}
