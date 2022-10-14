# -*- coding:utf-8 -*-
from pyspark.sql.session import SparkSession

from pyspark_api.initial_spark import get_spark_context

if __name__ == '__main__':
	spark: SparkSession = get_spark_context()
	rootPath = "xxxx"
	hdfs_path_apply = f"${rootPath}/apply"
	# spark.default.parallelism和spark.sql.shuffle.partitions用来配置并行度，已经水了两次了，这样不好
	spark.read.parquet(hdfs_path_apply)