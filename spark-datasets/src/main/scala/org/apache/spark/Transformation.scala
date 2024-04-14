package org.apache.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author Sam Ma
 * @date 2020/10/25
 * 使用spark rdd中的一些transformation函数: map()、flatmap()、sample()、mapPartition()
 */
object Transformation {

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

    /*
     * cogroup()和groupWith()操作：将多个rdd中具有相同key的value聚合在一起
     */
    val anotherRdd = spark.sparkContext.parallelize(Array[(Int, Char)]((1, 'f'), (3, 'g'), (2, 'h')), 3)
    val cogroupRdd = inputRdd.cogroup(anotherRdd, 3)
    // will gather: List((3,(CompactBuffer(c, e, f),CompactBuffer(g))), (4,(CompactBuffer(d),CompactBuffer())), (1,(CompactBuffer(a),CompactBuffer(f))), (2,(CompactBuffer(b, k, g, h),CompactBuffer(h))))
    logger.info(s"cogroup with two rdd, all elements of same key will gather: ${rddToString(cogroupRdd)}")

    /*
     * join()操作: 最终生成结果为相同key的元素放在一起<key, list<k, w>>
     */
    val joinRdd = inputRdd.join(anotherRdd, 3)
    // with anotherRdd, final result: List((3,(c,g)), (3,(e,g)), (3,(f,g)), (1,(a,f)), (2,(b,h)), (2,(k,h)), (2,(g,h)), (2,(h,h)))
    logger.info(s"inputRdd join with anotherRdd, final result: ${rddToString(joinRdd)}")

    /*
     * cartesian()操作: 求两个rdd的笛卡尔积, 若rdd1有m个分区 rdd2有n个分区, 则输出rdd1中m个分区与rdd2中n个分区两两合并后的结果
     */
    val rdd1 = spark.sparkContext.parallelize(Array[(Int, Char)]((1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')), 2)
    val rdd2 = spark.sparkContext.parallelize(Array[(Int, Char)]((1, 'A'), (2, 'B')), 2)
    val cartesianRdd = rdd1.cartesian(rdd2)
    // 总共4个分区有数据, cartesian with rdd1 and rdd2, final result: List(((1,a),(1,A)), ((2,b),(1,A)), ((1,a),(2,B)), ((2,b),(2,B)),
    // ((3,c),(1,A)), ((4,d),(1,A)), ((3,c),(2,B)), ((4,d),(2,B)))
    logger.info(s"cartesian with rdd1 and rdd2, final result: ${rddToString(cartesianRdd)}")

    /*
     * sortByKey()操作: 对rdd1中<K, V> record进行排序, 在相同key情况下 并不对value进行排序(ascending = true时, 表示按照key的升序排序)
     */
    val wordRdd = spark.sparkContext.parallelize(Array[(Char, Int)](('D', 2), ('B', 4), ('C', 3), ('A', 5), ('B', 2),
      ('C', 1), ('C', 3)))
    // 与reduceByKey()等操作根据Hash划分数据不同, sortByKey()为了保证生成rdd数据全局有序, 采用range划分数据
    val sortRdd = wordRdd.sortByKey(true, 2)
    // sorted all element in wordRdd, final result: List((A,5), (B,4), (B,2), (C,3), (C,1), (C,3), (D,2))
    logger.info(s"sorted all element in wordRdd, final result: ${rddToString(sortRdd)}")

    /*
     * coalesce()操作: 将rdd1的分区个数降低或升高为numPartitions, 对分区中数据重新partition (第二个参数是否shuffle，可减少数据倾斜问题)
     *    repartition(numPartitions)为重新分区, 其功能与coalesce(numPartitions, true)一样
     */
    val coalesceRdd = inputRdd.coalesce( 5, true)
    // uppercase coalesceRdd to 5 with shuffle, final result: List((3,e), (1,a), (2,b), (3,f), (2,k), (3,c),
    // (2,g), (4,d), (2,h))
    logger.info(s"adjust coalesceRdd partition number to 5 with shuffle, final result: ${rddToString(coalesceRdd)}")

    /*
     * repartitionAndSortWithinPartitions()操作: 将rdd1中的数据重新进行分区，分发到rdd2中. 其可以灵活的使用各种partitioner;
     *  而且对于rdd2中每个分区中的数据，按照key进行排序
     */
    val repartitionRdd = wordRdd.repartitionAndSortWithinPartitions(new HashPartitioner(3))
    // use HashPartitioner to resort all data in any partitions, reuslt: List((B,4), (B,2), (C,3), (C,1), (C,3), (A,5), (D,2))
    logger.info(s"use HashPartitioner to resort all data in any partitions, result: ${rddToString(repartitionRdd)}")

    /*
     * intersection()：将集和rdd1和rdd2中共同的元素抽取出来，从而形成新的集合rdd3
     */
    val numRdd1 = spark.sparkContext.parallelize(List(2, 2, 3, 4, 5, 6, 8, 6), 3)
    val numRdd2 = spark.sparkContext.parallelize(List(2, 3, 6, 6), 2)
    // use intersection operate to conjunct with numRdd1 and numRdd2, result: List(6, 3, 2)
    val intersectionRdd = numRdd1.intersection(numRdd2)
    logger.info(s"use intersection operate to conjunct with numRdd1 and numRdd2, result: ${rddToString(intersectionRdd)}")

    /*
     * distinct()、union()和zip()操作: distinct()用于对分区中数据去重、union()会将两个分区中数据拼接
     *  zip()将rdd1和rdd2中元素按照一一对应关系连接，构成<K, V>, 要求rdd1和rdd2中分区个数相同 且分区中元素数量相同
     *  zipPartitions(): 将rdd1和rdd2中的分区按照一一对应关系连接，要求rdd1和rdd2分区个数相同，不要求每个分区元素相同
     */
    val numRdd = spark.sparkContext.parallelize(1 to 8, 3)
    val charRdd = spark.sparkContext.parallelize('a' to 'h', 3)
    // mation$: zip with numRdd and charRdd, result: List((1,a), (2,b), (3,c), (4,d), (5,e), (6,f), (7,g), (8,h))
    val zipRdd = numRdd.zip(charRdd)
    logger.info(s"zip with numRdd and charRdd, result: ${rddToString(zipRdd)}")
    val sameRdd = spark.sparkContext.parallelize(Array[(Int, Char)]((3, 'g'), (2, 'h'), (4, 'i')), 3)
    val zipPartitionRdd = inputRdd.zipPartitions(sameRdd)({
      (rdd1Iter, rdd2Iter) => {
        var result = List[String]()
        while (rdd1Iter.hasNext && rdd2Iter.hasNext) {
          // 将rdd1和rdd2中的数据按照下划线连接，然后添加到result: list的首位
          result ::= rdd1Iter.next() + "_" + rdd2Iter.next()
        }
        result.iterator
      }
    })
    // zipPartitions with sameRdd and inputRdd, final: List((1,a)_(1,f), (3,c)_(3,g), (2,g)_(4,i), (3,f)_(2,h))
    logger.info(s"zipPartitions with sameRdd and inputRdd, final: ${rddToString(zipPartitionRdd)}")

    /*
     * zipWithIndex()和zipWithUniqueId()操作: zipWithIndex(): 对rdd1中的数据进行编号，编号方式是从0开始按序递增的;
     *   和zipWithUniqueId(): 对rdd1中的数据编号，编号方式为round-robin
     */
    val zipWithIndexRdd = inputRdd.zipWithIndex()
    val zipWithUniqueRdd = inputRdd.zipWithUniqueId()
    /* zipWithIndexRdd value: List(((1,a),0), ((2,b),1), ((2,k),2), ((3,c),3), ((4,d),4), ((3,e),5), ((3,f),6), ((2,g),7),
       ((2,h),8)), zipWithUniqueRdd value: List(((1,a),0), ((2,b),3), ((2,k),6), ((3,c),1), ((4,d),4),
       ((3,e),7), ((3,f),2), ((2,g),5), ((2,h),8)) */
    logger.info(s"zipWithIndexRdd value: ${rddToString(zipWithIndexRdd)}, zipWithUniqueRdd value: " +
      s"${rddToString(zipWithUniqueRdd)}")

    /*
     * subtractByKey()和subtract()操作: subtractByKey()会计算出key在rdd1中而不再rdd2中的record,
     *  subtract()则会计算在rdd1中而不在rdd2中的record
     */
    val subtractByKeyRdd = inputRdd.subtractByKey(sameRdd)
    // subtractByKey sameRdd from inputRdd, final rdd result: List((1,a))
    logger.info(s"subtractByKey sameRdd from inputRdd, final rdd result: ${rddToString(subtractByKeyRdd)}")
    val subtractRdd = inputRdd.subtract(sameRdd)
    logger.info(s"calculate record in inputRdd but not in sameRdd, result: ${rddToString(subtractRdd)}")

    /*
     * sortBy(func): 其基于func的计算结果对rdd1中的record进行排序
     */
    val sortByRdd = stringRdd.sortBy(record => record._2, true, 2)
    // sort all records in stringRdd with anonymous function, sortByRdd: List((1,a), (2,b), (3,c), (4,d), (2,e), (3,f), (2,g), (1,h))
    logger.info(s"sort all records in stringRdd with anonymous function, sortByRdd: ${rddToString(sortByRdd)}")

    /*
     * glom()操作，其会将rdd1中每个分区的record合并到一个list中
     */
    val glomRdd = stringRdd.glom()
    // gather all record of partition to list, List([Lscala.Tuple2;@f95d64d, [Lscala.Tuple2;@288728e, [Lscala.Tuple2;@b7d2d51)
    logger.info(s"gather all record of partition to list, ${rddToString(glomRdd)}")
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
