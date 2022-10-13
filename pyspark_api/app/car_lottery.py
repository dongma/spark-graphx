# -*- coding:utf-8 -*-
from pyspark.sql.session import SparkSession

from pyspark_api.initial_spark import get_spark_context

if __name__ == '__main__':
	spark: SparkSession = get_spark_context()
	rootPath = "xxxx"
	hdfs_path_apply = f"${rootPath}/apply"
	spark.read.parquet(hdfs_path_apply)