# -*- coding:utf-8 -*-
from pyspark.sql.session import SparkSession

from pyspark_api.initial_spark import get_spark_context

if __name__ == '__main__':
	spark: SparkSession = get_spark_context()
	textFile = spark.sparkContext.textFile("data/wikiOfSpark.txt")
	wordCount = (textFile.flatMap(lambda line: line.split(" ")).filter(lambda word: word != "") \
				 .map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y) \
				 .sortBy(lambda x: x[1], False).take(5))
	# [('Spark', 7), ('Apache', 4), ('the', 4), ('to', 3), ('ago', 2)]
	print(wordCount)