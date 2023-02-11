# -*- coding:utf-8 -*-
from pyspark import Accumulator
from pyspark.broadcast import Broadcast
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

from pyspark_api.initial_spark import get_spark_context
from hashlib import md5


def rdd_computes(sc: SparkContext) -> None:
	"""RDD常用的操作，sample(false, 0.1), repartition、coalesce"""
	parallel_rdd = sc.parallelize(list(range(0, 100)))
	print(f"sample(False, 0.1): {parallel_rdd.sample(False, 0.1).collect()}, partition length: "
		  f"{parallel_rdd.getNumPartitions()}")
	re_partition_rdd = parallel_rdd.repartition(4)  # 建议并行度可设置为CPU的2到3倍，coalesce(2)不会引起shuffle
	print(f"after call repartition: {re_partition_rdd.getNumPartitions()}, coalesce(2):"
		  f" {parallel_rdd.coalesce(2).toDebugString()}")


def map_partition_spword(partition, acc: Accumulator) -> tuple:
	"""对特殊的单词进行转换，并生成tuple元组数据"""
	md5_ = md5()
	for word in partition:
		md5_.update(word.encode("utf8"))
		if "spark" == word.lower():
			acc.add(1)
			yield md5_.hexdigest(), 2
		else:
			yield md5_.hexdigest(), 1


def aggregate_operators(sc: SparkContext) -> None:
	"""聚合函数，aggregate operators"""
	# 通过parallelize(array)创建rdd，groupByKey算子对元素进行分组
	words_rdd = sc.parallelize(["Spark", "is", "cool", "cool"])
	# result: {"spark": ["spark"], "is": ["is"], "cool": ["cool", "cool"]}, 展示iterator元素仍要探索
	group_key_rdd = words_rdd.map(lambda elem: (elem, elem)).groupByKey()

	# aggregateByKey(init, f1, f2)会指定函数，同一partition内先累加，不同分区相同key取value最大的
	aggregate_key = textFile.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)) \
		.aggregateByKey(0, lambda x, y: x + y, lambda x, y: max(x, y))
	print(aggregate_key.take(5))


if __name__ == '__main__':
	spark: SparkSession = get_spark_context()
	textFile = spark.sparkContext.textFile("data/wikiOfSpark.txt")
	sc: SparkContext = spark.sparkContext
	# 创建广播变量，只会在每个executor中存放一份数据，普通list则是每个task保存一份
	bc: Broadcast = sc.broadcast(["Apache", "Spark"])
	accumulator: Accumulator = sc.accumulator(0)
	# 将RDD元素转换为(Key, Value)的形式，但经过md5加密后，reduce之后就不正确了
	wordCount = (textFile.flatMap(lambda line: line.split(" ")).filter(lambda word: word in bc.value) \
				 .mapPartitions(lambda partition: map_partition_spword(partition, accumulator))
				 .reduceByKey(lambda x, y: x + y) \
				 .sortBy(lambda x: x[1], False).take(5))
	# [('Spark', 7), ('Apache', 4), ('the', 4), ('to', 3), ('ago', 2)]
	print(f"wordCount: {wordCount}, bc.value: {bc.value}, accumulator: {accumulator.value}")
	# sample(False, fraction)样例数据，
	rdd_computes(sc)
