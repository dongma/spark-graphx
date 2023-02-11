# -*- coding:utf-8 -*-
from pyspark.sql.session import SparkSession

from pyspark_api.initial_spark import get_spark_context


def join_dataset(spark: SparkSession) -> None:
	"""对数据集Dataset进行Join，演示inner、left、right join的SQL连接"""
	seq = [(1, "Mike", 28, "Male"), (2, "Lily", 30, "Female"), (3, "Raymond", 26, "Male"),
		   (5, "Dave", 36, "Male")]
	employee_rdd = spark.createDataFrame(seq, ["id", "name", "age", "gender"])
	salaries = [(1, 26000), (2, 30000), (4, 25000), (3, 20000)]
	salaries_rdd = spark.createDataFrame(salaries, ["id", "salary"])
	# inner join，[按id关联]只有当两边id都相同时，才展示数据记录
	inner_join_df = salaries_rdd.join(employee_rdd, "id", "inner")
	inner_join_df.show()
	# left join，[按id关联]左表数据全展示，右表id相同的才展示，否则字段全为null值
	left_join_df = salaries_rdd.join(employee_rdd, "id", "left")
	left_join_df.show()
	# right join,[按id关联]右表数据全展示，左表不存在id关联的以null展示
	right_join_df = salaries_rdd.join(employee_rdd, "id", "right")
	right_join_df.show()
	# left_semi（左半连接），其实只考虑left join的子集数据
	leftsemi_rdd = salaries_rdd.join(employee_rdd, "id", "leftsemi")
	leftsemi_rdd.show()


if __name__ == '__main__':
	spark: SparkSession = get_spark_context()
	# 在spark中针对dataset执行不同的join策略，并查看spark sql执行结果
	join_dataset(spark)