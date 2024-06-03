'''
Author: M1tsuha
Date: 2024-06-01 20:49:17
LastEditors: M1tsuha
LastEditTime: 2024-06-01 23:18:02
FilePath: \VSCProjects\Spark\Prac2_3_1.py
Description:

Copyright (c) 2024 by M1tsuha, All Rights Reserved. 
'''
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("MySQL Integration with DataFrame") \
    .config("spark.jars", "E:\Configure\Spark\spark-3.4.0-bin-hadoop3-scala2.13\jars\mysql-connector-j-8.0.33.jar") \
    .getOrCreate()

# 数据库连接属性
properties = {
    "user": "root",
    "password": "123456",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# 从数据库读取数据
df = spark.read.jdbc("jdbc:mysql://localhost:3306/testspark", "employee", properties=properties)

# 计算年龄的最大值和总和
age_max = df.agg({"age": "max"}).collect()[0][0]
age_sum = df.agg({"age": "sum"}).collect()[0][0]

print(f"Maximum age: {age_max}")
print(f"Total sum of ages: {age_sum}")

# 关闭 SparkSession
spark.stop()
