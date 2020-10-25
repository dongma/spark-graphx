package org.apache.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author Sam Ma
 * @date 2020/10/25
 * 使用spark rdd中的一些transformation函数: map()、flatmap()、sample()、mapPartition()
 */
object RDDTransformation {

  private[this] val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("rdd transformations")
      .config("spark.master", "local")
      .getOrCreate()

    /*
     * map()和mapValues()操作，map()用于对普通一纬元素依据func计算数值，mapValue()用于<key, value>
     *   格式的value进行重新计算
     */
    val inputRdd = spark.sparkContext.parallelize(Array[(Int, Char)]((1, 'a'), (2, 'b'),
      (2, 'k'), (3, 'c'), (4, 'd'), (3, 'e'), (3, 'f'), (2, 'g'), (2, 'h')), 3)
    // 使用"_"将entry<key, value>中的键值对拼接起来(结果为2_9, 1_h, 4_d)
    val concatRdd = inputRdd.map(record => record._1 + "_" + record._2)
    logger.info(s"all elements in concatRdd are: ${rddToString(concatRdd)}")
    val mapValueRdd = inputRdd.mapValues(value => value + "_")
    logger.info(s"append _ to value in map entry, result are: ${rddToString(mapValueRdd)}")

    /*
     * filter()和filterByRange()函数，filter的func函数对rdd中过滤(为true则保留)
     */
    val filterRdd = inputRdd.filter(record => record._1 % 2 == 0)
    logger.info(s"filter all elements which key couldn't subdivide 2: ${rddToString(filterRdd)}")
    val filterByRangeRdd = inputRdd.filterByRange(2, 4)
    logger.info(s"filter elements by key range {2 ~ 4}, elements : ${rddToString(filterByRangeRdd)}")

    /*
     * flatMap()和flatMapValues()操作，flatMap()与java 8中的功能相同, 用于将数组元素拍平
     *    flatMapValues()用于将<key, value>中的value数组元素进行拍平
     */
    val arrayRdd = spark.sparkContext.parallelize(Array[String]("how do you do", "are you ok",
      "thanks", "bye bye", "I'm ok"), 3)
    val flatMapRdd = arrayRdd.flatMap(element => element.split(" "))
    logger.info(s"all elements in flatMapRdd are: ${rddToString(flatMapRdd)}")
    val keyArrayRdd = spark.sparkContext.parallelize(Array[(Int, String)]((1, "how do you do"), (2, "are you ok"),
      (3, "thanks"), (4, "bye bye"), (5, "I'm ok")), 3)
    val flatMapKeyRdd = keyArrayRdd.flatMapValues(element => element.split(" "))
    logger.info(s"flat all value elements is: ${rddToString(flatMapKeyRdd)}")

    /*
     * sample()和sampleByKey()操作，其用于从rdd中进行数据抽样，按照百分比抽取数据. 其中withReplacement表示是否有放回
     */
    val sampleRdd = inputRdd.sample(false, 0.5)
    val withReplaceRdd = inputRdd.sample(true, 0.5)
    logger.info(s"sample data with 0.5 percentage in inputRdd are: ${rddToString(sampleRdd)}")

    /*
     * mapPartitions()和mapPartitionsWithIndex()操作，对rdd中的每个分区进行操作并输出一组数据
     */
    val numberRdd = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
    val mapPartitionsWithIndexRdd = numberRdd.mapPartitionsWithIndex((pid, iter) => {
      var elements = List[String]()
      var odd = 0
      var even = 0
      // 将每个分区中的奇数和偶数累加，并将其放入elements列表中
      while (iter.hasNext) {
        val element = iter.next()
        if (element % 2 == 0) {
          even += element
        } else {
          odd += element
        }
      }
      // 将(pid, odd)、(pid, even)存放到elements中, 将收集到的数据使用iterator进行返回
      elements = elements :+ "pid = " + pid + ", odd = " + odd
      elements = elements :+ "pid = " + pid + ", even = " + even
      elements.iterator
    })
    // 20/10/25 17:27:47 INFO RDDTransformation$: mapPartitions function in numberRdd result is: List(4, 2, 5, 10, 16, 8)
//    logger.info(s"mapPartitions function in numberRdd result is: ${rddToString(mapPartitionsRdd)}")
    logger.info(s"mapPartitionsWithIndex function in numberRdd result is: ${rddToString(mapPartitionsWithIndexRdd)}")

    /*
     * partitionBy()依据分区策略对Rdd进行重新的分区，spark支持的HashPartition和RangePartition
     */
    val hashPartition = inputRdd.partitionBy(new HashPartitioner(2))
    val rangePartition = inputRdd.partitionBy(new RangePartitioner(2, inputRdd))
    // 20/10/25 17:41:44 INFO RDDTransformation$: hash partition: 2, range partition: 2
    logger.info(s"hash partition: ${hashPartition.getNumPartitions}, range partition: ${rangePartition.getNumPartitions}")

    /*
     * groupByKey()将rdd中的<K, V> record按照Key聚合在一起，形成<Key, List<Value>>格式数据
     */
    val groupByKeyRdd = inputRdd.groupByKey(2)
    // RDDTransformation$: groupByKey to calculate rdd value: List((4,CompactBuffer(d)), (2,CompactBuffer(b, e, g)), (1,CompactBuffer(a, h)), (3,CompactBuffer(c, f)))
    logger.info(s"groupByKey to calculate rdd value: ${rddToString(groupByKeyRdd)}")

    /*
     * reduceByKey(func, [numPartitions])用于在聚合过程中使用func对这些record的value进行融合计算(用func对所有list进行计算)
     */
    val stringRdd = spark.sparkContext.parallelize(Array[(Int, String)]((1, "a"),
      (2, "b"), (3, "c"), (4, "d"), (2, "e"), (3, "f"), (2, "g"), (1, "h")), 3)
    val reduceByKeyRdd = stringRdd.reduceByKey((x, y) => x + "_" + y, 2)
    logger.info(s"reduceByKey operate in stringRdd result: ${rddToString(reduceByKeyRdd)}")

    /*
     * aggregateByKey(zeroValue)(seqOp, combOp, [numPartitions]): 依据key值使用seqOp和combOp处理数据
     *  seqOp: 类似与map-reduce中的map阶段，对key相同的元素执行seqOp操作
     *  combOp: 与map-reduce中的reduce阶段相同，对seqOp的结果进行combOp操作
     */
    val aggregateByKeyRdd = stringRdd.aggregateByKey("x", 2)(_ + "_" + _, _ + "@" + _)
    // sformation$: aggregate value by key, final result rdd is: List((4,x_d), (2,x_b@x_e@x_g), (1,x_a@x_h), (3,x_c@x_f))
    logger.info(s"aggregate value by key, final result rdd is: ${rddToString(aggregateByKeyRdd)}")

    /*
     * combineByKey(createCombiner, mergeValue, mergeCombiners, [numPartitions]): 通用的基础聚合操作
     */
    val combineByKeyRdd = inputRdd.combineByKey((v: Char) => {
      if (v == 'c') {
        v + "0"
      } else {
        v + "1"
      }
    },
      (c: String, v: Char) => c + "+" + v,
      (c1: String, c2: String) => c1 + "_" + c2,
      2)
    // RDDTransformation$: use combineByKey to aggregate value: List((4,d1), (2,b1+k_g1+h), (1,a1), (3,c0+e_f1))
    logger.info(s"use combineByKey to aggregate value: ${rddToString(combineByKeyRdd)}")

    /*
     * foldByKey(): foldByKey()是一个简化的aggregateByKey()，seqOp和combineOp共用一个function
     */
    val foldByKeyRdd = stringRdd.foldByKey("x", 2)((x, y) => x + "_" + y)
    // ormation$: use foldByKey() to aggregate all elements to Rdd, foldByKeyRdd: List((4,x_d), (2,x_b_x_e_x_g), (1,x_a_x_h), (3,x_c_x_f))
    logger.info(s"use foldByKey() to aggregate all elements to Rdd, foldByKeyRdd: ${rddToString(foldByKeyRdd)}")
  }

  /** 用于将rdd转换为Seq数组并将其中的元素在console中进行展示 */
  private[this] def rddToString[T](rdds: RDD[T]): Seq[String] = {
    var elements = Seq.empty[String]
    // 直接调用rdd.foreach时会清空elements中的所有元素，是非常奇怪的现象
    rdds.collect().foreach(ele => {
      elements = elements :+ String.valueOf(ele)
    })
    elements
  }

  /** 用于将rdd的数据按分区进行统计，将每个分区内的元素收集到List中 */
  private[this] def partitionRddToString[T](partitionRdd: RDD[T]): Seq[(Int, List[String])] = {
//    partitionRdd.pa
    Seq.empty
  }

}
