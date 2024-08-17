# -*- coding:utf-8 -*-
from pyspark.sql.session import SparkSession


def get_spark_context() -> SparkSession:
    """用于创建spark session对象"""
    builder = SparkSession.builder.appName("pandas-on-spark")\
        .master("local[*]")
    # bugfix in graphframe_0.8.1 version，设置executor和driver占用内存的大小
    builder = builder.config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
        .config("spark.executor.memory", "4g")\
        .config("spark.driver.memory", "8g").config("spark.executor.cores", "4")
    # Pandas API on Spark automatically uses this Spark session with the configurations set.
    return builder.getOrCreate()