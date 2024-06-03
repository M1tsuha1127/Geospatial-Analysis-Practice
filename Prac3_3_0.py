'''
Author: M1tsuha
Date: 2024-06-01 20:49:17
LastEditors: M1tsuha
LastEditTime: 2024-06-01 21:17:44
FilePath: \VSCProjects\Spark\Prac2_3.py
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

# 定义数据模式
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True)
])

# 设置要插入的数据
data = [
    (3, "Mary", "F", 26),
    (4, "Tom", "M", 23)
]

# 创建 DataFrame
employee_df = spark.createDataFrame(data, schema)

# 数据库连接属性
properties = {
    "user": "root",
    "password": "123456",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# 把数据写入数据库
employee_df.write.jdbc("jdbc:mysql://localhost:3306/testspark", "employee", "append", properties)

# 关闭 SparkSession
spark.stop()
