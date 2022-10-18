# -*- coding:utf-8 -*-
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import split, explode
from pyspark.sql.session import SparkSession

from pyspark_api.initial_spark import get_spark_context

if __name__ == '__main__':
	spark: SparkSession = get_spark_context()
	stream_df: DataFrame = spark.readStream.format("socket").option("host", "127.0.0.1") \
		.option("port", "9999").load()
	# 使用DataFrame Api完成Word count的计算，接收到的单词以" "作为分割，得到单词组words (fix,value取值有问题)
	stream_df = stream_df.withColumn("words", split("value", " "))\
		.withColumn("word", explode("words"))\
		.groupBy("word").count()
	# 将word count结果写到console中，输出模式为"complete"
	stream_df.writeStream.format("console").option("truncate", False)\
		.outputMode("complete").start()\
		.awaitTermination()