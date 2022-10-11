# -*- coding:utf-8 -*-
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

from pyspark_api.initial_spark import get_spark_context
from hashlib import md5


def map_partition_spword(partition) -> tuple:
	"""对特殊的单词进行转换，并生成tuple元组数据"""
	md5_ = md5()
	for word in partition:
		md5_.update(word.encode("utf8"))
		if "spark" == word:
			yield md5_.hexdigest(), 2
		else:
			yield md5_.hexdigest(), 1


if __name__ == '__main__':
	spark: SparkSession = get_spark_context()
	textFile = spark.sparkContext.textFile("data/wikiOfSpark.txt")
	# 将RDD元素转换为(Key, Value)的形式，但经过md5加密后，reduce之后就不正确了
	wordCount = (textFile.flatMap(lambda line: line.split(" ")).filter(lambda word: word != "") \
				 .mapPartitions(lambda word: map_partition_spword(word)).reduceByKey(lambda x, y: x + y) \
				 .sortBy(lambda x: x[1], False).take(5))
	# [('Spark', 7), ('Apache', 4), ('the', 4), ('to', 3), ('ago', 2)]
	print(wordCount)

	# 通过parallelize(array)创建rdd，groupByKey算子对元素进行分组
	sc: SparkContext = spark.sparkContext
	wordsRdd = sc.parallelize(["Spark", "is", "cool", "cool"])
	# result: {"spark": ["spark"], "is": ["is"], "cool": ["cool", "cool"]}, 展示iterator元素仍要探索
	groupKeyRdd = wordsRdd.map(lambda elem: (elem, elem)).groupByKey()

	# aggregateByKey(init, f1, f2)会指定函数，同一partition内先累加，不同分区相同key取value最大的
	aggregateKey = textFile.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)) \
		.aggregateByKey(0, lambda x, y: x + y, lambda x, y: max(x, y))
	print(aggregateKey.take(5))
