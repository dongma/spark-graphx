package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * @author Sam Ma
 * @date 2020/06/22
 * 在apache spark中操作RDDs结果集，包括distinct filter map flatMap等操作
 */
object RDDsInSpark {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("databricks spark application")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._
    // converts a Dataset[long] to RDD[long]
    spark.range(50).rdd
    // to operate this data, you will need to convert this Row object to the correct data type or extract value
    spark.range(10).toDF().rdd.map(rowObject => rowObject.getLong(0))
    // you can use some methodology to create a DataFrame or Dataset from an RDD
    spark.range(10).rdd.toDF()

    // 可以从一个Collection中创建RDD, 你应显示地指定partitions分区地数量，可以显示地指定rdd名称
    val wordsCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
    val wordsRDD = spark.sparkContext.parallelize(wordsCollection, 2)
    wordsRDD.setName("wordRDD")

    // 可以使用textFile(path) Api或者wholeTextFiles(somePath)从数据源中加载RDD数据，read data as rdds
    //    spark.sparkContext.textFile("/some/path/withTextFiles")
    // each text file should become a single record, the use case here would be where each file is a file
    //    spark.sparkContext.wholeTextFiles("/some/path/withTextFiles")

    // 在rdd上调用distinct()方法对数据集进行去重, 可自定义filter函数对wordsRDD数据进行过滤
    val distinctCount = wordsRDD.distinct().count()
    val wordsArray = wordsRDD.filter(word => startsWithS(word)).collect()
    logger.info(s"distinct count value: $distinctCount, filter item whether start with S array: $wordsArray")

    // 调用map会将mapWordRdd转换为三元组数据结构，(word, word[0], boolean)
    val mapWordRdd = wordsRDD.map(word => (word, word(0), word.startsWith("S")))
    val filterValue = mapWordRdd.filter(record => record._3).take(5)
    logger.info(s"filter wordRdd which start with S: $filterValue")

    val flatMapValue = wordsRDD.flatMap(word => word.toSeq).take(5)
    logger.info(s"flatMapValue is: $flatMapValue")
    // 对wordsRDD中的记录进行sort排序，按照word.length长度进行比较
    wordsRDD.sortBy(word => word.length() - 1).take(2)

    // 对native RDD进行切分，将其按照50%的比例切分 最终会返回RDDs Array数组
    val fiftyFiftySplit = wordsRDD.randomSplit(Array[Double](0.5, 0.5))
    // 使用reduce()函数对1～20中整数进行累加，生成reduce结果
    val reduceValue = spark.sparkContext.parallelize(1 to 20).reduce(_ + _)
    logger.info(s"apache rdd split with 50% size: ${fiftyFiftySplit.length}, reduce value is: $reduceValue")
    // 按照word.length对rdd中word进行排序，并生成最终排序的RDD结果
    wordsRDD.reduce(wordLengthReducer)

    // RDD中的first()方法以及max()和min()的方法，通过max()和min()函数获取rdd中的最大、最小值
    val firstWord = wordsRDD.first()
    val maxValue = spark.sparkContext.parallelize(1 to 20).max()
    val minValue = spark.sparkContext.parallelize(1 to 20).min()
    logger.info(s"first word: $firstWord, minValue is $minValue, maxValue is $maxValue")

    // 为了将RDD处理后的结果进行保存，可调用saveAsTextFile(path) 可指定压缩的策略 BZip2Codec(方法不适配)、序列化为文件
//    wordsRDD.saveAsTextFile("file:/tmp/wordrdd")
    //    wordsRDD.saveAsTextFile("file:/tmp/wordrdd-compressed", classOf[BZip2Codec])
//    wordsRDD.saveAsObjectFile("/tmp/wordrdd-sequence")

    // 可调用cache()方法将RDDs数据缓存在内存中，storage.StorageLevel其是memory only、disk only和separately的结合
    wordsRDD.cache()
    logger.info(s"words.getStorageLevel value is: ${wordsRDD.getStorageLevel}")
    // checkPointing is the act of saving an RDD to desk so that future references to this RDD point to get from disk
    // rather than recomputing the RDD from its origin source，此种方式可用于应用性能的优化
    spark.sparkContext.setCheckpointDir("/tmp/path/checkpoint")
    wordsRDD.checkpoint()

    // 可以使用wordsRDD.pipe("wc -l")方法对word单词数量进行统计
    val wordLines = wordsRDD.pipe("wc -l").collect()
    logger.info(s"pipe commnd wc -l result: $wordLines")
    // 要对RDD中记录进行处理可使用mapPartition()或者foreachPartition，withIndex可携带map的index下标索引值
    val mapSize = wordsRDD.mapPartitions(part => Iterator[Int](1)).sum()
    val indexValue = wordsRDD.mapPartitionsWithIndex(indexedFunction).collect()
    logger.info(s"mapSize value is: $mapSize, indexValue is: $indexValue")
    // 使用foreachPartition对rdd中的数据进行consume，并将rdd中数据写入到file system
    wordsRDD.foreachPartition { iter =>
      import java.io._

      import scala.util.Random
      val randomFileName = new Random().nextInt()
      val pw = new PrintWriter(new File(s"/tmp/random-file-${randomFileName}.txt"))
      while (iter.hasNext) {
        pw.write(iter.next())
      }
      pw.close()
    }

    // glom()函数会将各分区dataset中数据收集并将其转换成为Arrays，it's simple to crash the driver, 最终结果类型Array(Array(hello), Array(world))
    val glomResult = spark.sparkContext.parallelize(Seq("hello", "world"), 2).glom().collect()

    // 将wordRDD通过map()转换为Map类型，其key为word lowerCase value值固定为1
    val wordMap = wordsRDD.map(word => (word.toLowerCase, 1))
    // 按word字符的首字母对wordsRDD数据集进行分组，key为word的首字母
    val keyword = wordsRDD.keyBy(word => word.toLowerCase.toSeq(0).toString)
    // mapValues()会对word进行归集，对value部分做重新的转换, flatMap()方法value是数组形式的Entry拍平
    val mapValues = keyword.mapValues(word => word.toUpperCase).collect()
    logger.info(s"keyword.mapValues mapValues: $mapValues")
    // 要提取出keyword中所有的key和value数据，可以使用keys.collect()和values.collect()进行处理
    val allkeys = keyword.keys.collect()
    val allvalues = keyword.values.collect()
    // 若要根据key从map中获取相应的value 可使用lookup()方法
    val seqValue = keyword.lookup("s")
    logger.info(s"all keys: $allkeys and all values: $allvalues in keyword, lookup(s) value: $seqValue")

    val distinctChars = wordsRDD.flatMap(word => word.toLowerCase.toSeq).distinct.collect()
    import scala.util.Random
    val sampleMap = distinctChars.map(c => (c, new Random().nextDouble())).toMap
    // 可以使用sampleByKey()和sampleByKeyExact()对RDD中的数据进行抽样检查，目前暂时不理解
    val sampleByKeyMap = wordsRDD.map(word => (word.toLowerCase.toSeq(0), word))
      .sampleByKey(true, sampleMap, 6L)
      .collect()
    logger.info(s"rdd.sampleByKey value is: $sampleByKeyMap")
    val sampleByKeyExactMap = wordsRDD.map(word => (word.toLowerCase.toSeq(0), word))
      .sampleByKeyExact(true, sampleMap, 6L).collect()
    logger.info(s"rdd.sampleByKeyExact value is: $sampleByKeyExactMap")

    // 在scala中对wordsRDD进行转换，并以letter作为map()的key值 可调用countByKey()对chars数据进行统计
    val chars = wordsRDD.flatMap(word => word.toLowerCase.toSeq)
    val KVcharacters = chars.map(letter => (letter, 1))
    KVcharacters.countByKey()

    // combineByKey() 可以指定一个combiner，在给定key上根据一些函数合并所有的values，可以指定output partitions分区的数量
    val valToCombiner = (value: Int) => List(value)
    val mergeValueFunc = (vals: List[Int], valToAppend: Int) => valToAppend :: vals
    val mergeCombinerFunc = (vals1: List[Int], vals2: List[Int]) => vals1 ::: vals2
    // now we define these as function variables
    val outputPartitions = 6
    KVcharacters.combineByKey(valToCombiner, mergeValueFunc, mergeCombinerFunc, outputPartitions).collect()
    // foldByKey会针对每个key使用一个associated关联函数合并其所有values
    val foldValues = KVcharacters.foldByKey(0)(addFunc).collect()
    logger.info(s"KVcharacters.foldByKey with add function foldValues: $foldValues")
    // zips 允许将两个RDD结合起来，第一个rdd中的item会作为key numRange中的元素作为value, 会假定两个rdd的长度相同
    val numRange = spark.sparkContext.parallelize(0 to 9, 2)
    val zipMapValues = wordsRDD.zip(numRange).collect()
    println("wordsRDD.zip(numRange) values: " + zipMapValues)

    val allretailDataDf = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("/Users/madong/datahub-repository/spark-graphx/example-data/retail-data/all")
    val retailRowRdd = allretailDataDf.coalesce(10).rdd
    allretailDataDf.printSchema()
    // 对allretailDataDf中的数据进行转换 map取第6个字段,最后取前5条记录 并对其进行println展示
    retailRowRdd.map(row => row(6)).take(5).foreach(println)
    val keyedRdd = retailRowRdd.keyBy(row => row(6).asInstanceOf[Int].toDouble)
    val hashPartitionerValue = keyedRdd.partitionBy(new HashPartitioner(10)).take(10)
    logger.info(s"keyedRdd.partitionBy HashPartitioner value: $hashPartitionerValue")

    // 使用apache spark自定义分区DomainPartitioner来获取数据，glom()方法不理解
    val rowDatas = keyedRdd
      .partitionBy(new DomainPartitioner).map(_._1).glom().map(_.toSet.toSeq.length)
      .take(5)
    logger.info(s"keyedRdd.partitionBy(new DomainPartitioner) values: $rowDatas")
    // 在spark中想parallelize操作的任何对象都必须序列化，实现java Serialize接口效率太慢 其推荐使用Apache Kyro
    val sparkConf = new SparkConf().setMaster("local").setAppName("spark Kyro Serialize")
    sparkConf.registerKryoClasses(Array(classOf[Flight], classOf[FlightWithMetadata]))
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.parallelize(1 to 10).map(num => new FlightWithMetadata(num, num * 3)).toDF()
  }

  /**
   * filter函数判断rdd中item是否是以S字母开头
   *
   * @param individual
   * @return
   */
  def startsWithS(individual: String) = {
    individual.startsWith("S")
  }

  /**
   * 根据字符串word的长度在RDD对其进行reducer
   *
   * @param leftWord
   * @param rightWord
   * @return
   */
  def wordLengthReducer(leftWord: String, rightWord: String): String = {
    if (leftWord.length > rightWord.length) {
      leftWord
    } else {
      rightWord
    }
  }

  /**
   * 定义index function的函数
   *
   * @param partitionIndex
   * @param withinPartIterator
   * @return
   */
  def indexedFunction(partitionIndex: Int, withinPartIterator: Iterator[String]) = {
    withinPartIterator.toList.map(
      value => s"Partition: $partitionIndex => $value"
    ).iterator
  }

  def addFunc(left: Int, right: Int) = left + right

}
