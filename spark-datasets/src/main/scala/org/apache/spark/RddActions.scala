package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author Sam Ma
 * @date 2020/10/29
 * 使用定义spark rdd的一些action()数据归集动作，action对触发rdd流程真正的执行
 */
object RddActions {

  private[this] val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark actions")
      .config("spark.master", "local")
      .getOrCreate()

    /*
     * spark action()一般用于提交计算job(), 经过一连串的transformation()后使用; count()、countByKey()、countByValue()的actions
     */
    val dataRdd = spark.sparkContext.parallelize(Array[(Int, Char)]((3, 'c'), (3, 'f'), (5, 'e'), (4, 'd'),
      (2, 'b'), (5, 'e'), (2, 'g')), 3)
    val size = dataRdd.count()
    val countByKey = dataRdd.countByKey()
    val countByValue = dataRdd.countByValue()
    // RddActions$: element size: 7, countByKey value: Map(3 -> 2, 4 -> 1, 5 -> 2, 2 -> 2),
    // countByValue: Map((4,d) -> 1, (5,e) -> 2, (3,f) -> 1, (3,c) -> 1, (2,g) -> 1, (2,b) -> 1)
    logger.info(s"element size: $size, countByKey value: $countByKey, countByValue: $countByValue")

    /*
     * collect()和collectAsMap(),都是用于将rdd中的元素收集起来 map<key, value>格式数据使用collectAsMap()
     */
    val collectMap = dataRdd.collectAsMap()
    val keySet = dataRdd.keys.collect()
    // all keySet in dataRdd: [I@23e3f5cd, gather all data in dataRdd: Map(2 -> g, 5 -> e, 4 -> d, 3 -> f)
    logger.info(s"all keySet in dataRdd: $keySet, gather all data in dataRdd: $collectMap")

    /*
     * foreach()和foreachPartition()操作, 其用于将rdd中的数据处理(按分区、普通处理)
     */
    dataRdd.foreach(element => print(element._2))
    // 从实际输出来看，foreach()和foreachPartition()的效果是一致的，看不出什么区别
    dataRdd.foreachPartition(iterator => {
      while (iterator.hasNext) {
        val element = iterator.next()
        if (element._1>= 3)
          println(element)
      }
    })

    /*
     * fold() reduce()和aggregate()操作，这3个方法都是用于将rdd中的element元素收集在一起, 这3个函数都会按分
     *  区处理数据，分区计算完成后再将各分区拼在一起
     */
    val dataRdd3 = spark.sparkContext.parallelize(Array[String]("a", "b", "c", "d", "e", "f",
      "g", "h", "i"), 3)
    // 07:16:28 INFO RddActions$: use fold function to gather all value: 0_0_a_b_c_0_d_e_f_0_g_h_i
    val foldValue = dataRdd3.fold("0")((x, y) => x + "_" + y)
    logger.info(s"use fold function to gather all value: $foldValue")
    val reduceValue = dataRdd3.reduce((x, y) => x + "_" + y)
    // INFO RddActions$: use reduce function to calculate all value: a_b_c_d_e_f_g_h_i
    logger.info(s"use reduce function to calculate all value: $reduceValue")
    val aggregateValue = dataRdd3.aggregate("0")((x, y) => x + "_" + y, (x, y)=> x + "=" + y)
    // INFO RddActions$: use aggregate function to gather all elements: 0=0_a_b_c=0_d_e_f=0_g_h_i
    logger.info(s"use aggregate function to gather all elements: $aggregateValue")

    /*
     * 1) treeAggregate()和treeReduce()操作: 将rdd1中的record按树形结构聚合，seqOp和combOp语义与aggregate()
     *   中相同，树的高度为2;
     * 2) treeReduce()会将record按树形结构聚合，func语义与reduce(func)中相同;(不是很理解)
     */
    val dataP6Rdd = spark.sparkContext.parallelize(1 to 18, 6).map(x => x + "")
    val treeAggValue = dataP6Rdd.treeAggregate("0")((x, y) => x + "_" + y, (x, y) => x + "=" + y)
    logger.info(s"use treeAggregate method to gather all element, value: $treeAggValue")
    val treeReduceValue = dataP6Rdd.treeReduce((x, y) => x + "_" + y)
    logger.info(s"use tree reduce method to gather all element, result: $treeReduceValue")

    /*
     * reduceByKeyLocality()操作: 其会将rdd1中的record按照key进行排序，不同于reduceByKey() 其首先会在本地进行局部reduce(),
     *  然后会把数据汇总到Driver端进行全局reduce(), 最终返回的结果放在hashmap中
     */
    val mapRdd = spark.sparkContext.parallelize(Array[(Int, String)]((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"),
      (3, "f"), (2, "g"), (1, "h"), (2, "i")), 3)
    // 11:32:02 INFO RddActions$: use reduceByKeyLocally to reduce records, final result: Map(1 -> a_h, 2 -> b_g_i, 3 -> c_f, 4 -> d, 5 -> e)
    val reduceByKeyLocality = mapRdd.reduceByKeyLocally((x, y) => x + '_' + y)
    logger.info(s"use reduceByKeyLocally to reduce records, final result: $reduceByKeyLocality")

    /*
     * take(）、first()、takeOrdered()、top()操作，其中takeOrdered(num)会取出较小的几个，而top(num)取出较大几个
     */
    // INFO RddActions$: take(1) value: [Lscala.Tuple2;@4b195203 , first() value: (1,a) , takeOrdered(2): [Lscala.Tuple2;@4a642e4b, top(2): [Lscala.Tuple2;@6b162ecc
    logger.info(s"take(1) value: ${mapRdd.take(1)} , first() value: ${mapRdd.first()} , " +
      s"takeOrdered(2): ${mapRdd.takeOrdered(2)}, top(2): ${mapRdd.top(2)}")

    /*
     * isEmpty(): 判断rdd中是否存在数据，lookup(key)返回list数据：找出rdd中包含特定key的value值,
     *  将这些value形成list
     */
    // /10/31 11:41:46 INFO RddActions$: whether isEmpty(): false, lookup function(): WrappedArray(b, g, i)
    logger.info(s"whether isEmpty(): ${mapRdd.isEmpty()}, lookup function(): ${mapRdd.lookup(2)}")
  }

}
