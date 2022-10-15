# -*- coding:utf-8 -*-
from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from pyspark_api.initial_spark import get_spark_context


def calculate_rate(spark: SparkSession, root_path) -> None:
	"""计算小汽车摇号中签的概率"""
	# 北京小汽车摇号dataset放在hdfs /car_dataset的目录下，其中apply表示申请者号码，lucky表示已摇中的号码
	apply_number_df = spark.read.parquet(f"{root_path}/apply")
	lucky_dogs_df = spark.read.parquet(f"{root_path}/lucky")
	# 过滤2016年以后的中签数据，且仅抽取中签号码的carNum字段
	filter_lucky_dogs = (
		lucky_dogs_df.filter(lucky_dogs_df["batchNum"] >= "201601").select("carNum")
	)
	# 摇号数据和中签数据做内关联，Join key为中签号码carNum
	joint_df = apply_number_df.join(filter_lucky_dogs, "carNum", "inner")
	# 以batchNum、carNum做分组，统计倍率系数
	multipliers = (joint_df.groupBy(["batchNum", "carNum"]).agg(count("batchNum").alias("multiplier")))
	# 以carNum作分组，保留最大的倍率系数
	unique_multipliers = (multipliers.groupBy("carNum").agg(max("multiplier").alias("multiplier")))
	# 以multiplier倍率做分组，统计人数，agg()聚合后展示前20条
	result = (unique_multipliers.groupBy("multiplier").agg(count("carNum").alias("cnt"))
			  .orderBy("multiplier"))
	result.show(20)


def create_dataframes(spark: SparkSession) -> None:
	"""从不同datasource数据源加载数据，并创建DataFrame对象"""
	seq = [("bob", 14), ("alice", 18)]
	person_rdd = spark.sparkContext.parallelize(seq)
	person_df = spark.createDataFrame(person_rdd, ['name', 'age'])
	person_df.show()
	# 也可以直接调用person_rdd.toDF([column...])将rdd转换为DataFrame对象
	another_df = person_rdd.toDF(['name', 'age'])
	another_df.show()
	# 先依据StructType#StructField定义schema约束，然后从hdfs加载csv文件
	schema = StructType([
		StructField("DEST_COUNTRY_NAME", StringType(), True),
		StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
		StructField("count", IntegerType(), True)
	])
	summary_csv_path = "/Users/madong/datahub-repository/spark-graphx/example-data/flight-data/csv/2010-summary.csv"
	summary_df = spark.read.format("csv").schema(schema).option("header", True)\
		.option("mode", 'dropMalformed').load(f"file:///{summary_csv_path}")
	summary_df.show(10)


if __name__ == '__main__':
	spark: SparkSession = get_spark_context()
	rootPath = "car_dataset/car_lottery_2011_2019"
	# calculate_rate(spark, rootPath)
	# spark与不同数据源整合，创建DataFrame数据结构
	create_dataframes(spark)