package org.apache.spark.runner

import org.apache.spark.helper.CsvDfHelper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.{GraphUtils, LocalSpark}

/**
 * 用spark计算企业的实际控制人，对主体企业的投资比例大于等于0.3
 *
 * @author Sam Ma
 * @date 2023/04/05
 */
class ActualRunner {

  /** 根据点、边的csv数据构造Graph图，对投资比例按股权进行累加与计算 */
  def generateGraph(spark: SparkSession): DataFrame = {
    val csvDfHelper: CsvDfHelper = CsvDfHelper.create(spark)
    // 从hadoop上加载person、company的csv数据文件，注册为spark临时视图
    csvDfHelper.registerDf("person", "/dataset")
    csvDfHelper.registerDf("company", "/dataset")

    val vertexDf = spark.sql(
      """select p.keyNo as id, p.name, p.type from person p
        | union
        |select c.keyNo as id, c.name, c.label as type from company c
        |""".stripMargin)
    vertexDf.createOrReplaceTempView("vertex")
    //    vertexDf.show(10)
    val investDf = getInvestEdge(spark, csvDfHelper)

    GraphUtils.point2pointRight(vertexDf, investDf, 8, 0.001)
      .filter(row => row.getAs[Double]("prop") > 0.3)
  }

  /** 获取投资边，关键字段为：from_id、to_id及rate（按小数点）投资比例 */
  def getInvestEdge(spark: SparkSession, csvDfHelper: CsvDfHelper): DataFrame = {
    val vidMapDf = spark.sql(
      """select distinct id as v_md5, keyNo as vid from person
        |union all select distinct id as v_md5, keyNo as vid from company""".stripMargin)
    vidMapDf.createOrReplaceTempView("vertex_idmap")
    //    vidMapDf.show(10)
    // 加载关系边的数据，关系边中的from、to并不是对应节点id(为md5)，要进行处理替换下
    csvDfHelper.registerDf("pinvest_rel", "/dataset")
    csvDfHelper.registerDf("cinvest_rel", "/dataset")

    val investDf = spark.sql(
      """select pi.id as id_md5, pi.eid as id, pi.type, pi.rate/100 as rate, fvi.vid as from_id, tvi.vid as to_id
        |  from pinvest_rel pi
        |     left join vertex_idmap as fvi on pi.from == fvi.v_md5
        |     left join vertex_idmap as tvi on pi.to == tvi.v_md5
        | union
        | select ci.id as id_md5, ci.eid as id, ci.type, ci.holder/100 as rate, fvi.vid as from_id, tvi.vid as to_id
        |  from cinvest_rel ci
        |     left join vertex_idmap as fvi on ci.from == fvi.v_md5
        |     left join vertex_idmap as tvi on ci.to == tvi.v_md5
        | """.stripMargin)
    //    investDf.show(20)
    investDf
  }

}


object ActualRunner extends LocalSpark {

  def create(): ActualRunner = {
    new ActualRunner()
  }

  def main(args: Array[String]): Unit = {
    withSession("actual Runner", { spark: SparkSession =>
      val dmActual: DataFrame = ActualRunner.create().generateGraph(spark)
//      dmActual.show(10)
      dmActual.createOrReplaceTempView("actual_person")
      val dataset: DataFrame = spark.sql(
        """select ap.from_id, v1.name as from_name,
          | ap.to_id, v2.name as to_name,
          | ap.prop
          | from actual_person ap
          |left join vertex v1 on ap.from_id == v1.id
          |left join vertex v2 on ap.to_id == v2.id""".stripMargin)
/*    +--------------+------------------------+--------------+----------------------------+----+
      |       from_id|               from_name|         to_id|                     to_name|prop|
      +--------------+------------------------+--------------+----------------------------+----+
      |11237796588025|                    徐驰|10136132161012|天津茶源餐饮管理服务有限公司|0.95|
      |10091102063629|广州本宫餐饮服务有限公司|10136132161012|天津茶源餐饮管理服务有限公司|0.95|
      |11237796588027|                  睢宏江|10136132161012|天津茶源餐饮管理服务有限公司|0.95|
      |11237796588048|                  胡秀川|31225952294810|    云南星果文化传播有限公司|0.85|
      |11237796588023|                    李哲|33363626168496|  江苏乐尔达食品科技有限公司| 0.4|
      |11237796588025|                    徐驰|10091102063629|    广州本宫餐饮服务有限公司|0.48|
      |11237796588027|                  睢宏江|10091102063629|    广州本宫餐饮服务有限公司|0.44|
      |11237796588025|                    徐驰|18605621136430|      广州越秀区**石美饮品店| 1.0|
      |11237796588047|                  茶世斌|30174481594112|仙浴泉酒店管理(云南)有限公司| 1.0|
      |11237796588047|                  茶世斌|30442928894541|      茶星科技(云南)有限公司| 0.9|
      +--------------+------------------------+--------------+----------------------------+----+
      only showing top 10 rows*/
      dataset.show(10)
    })
  }

}
