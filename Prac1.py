'''
Author: M1tsuha
Date: 2024-05-16 14:14:13
LastEditors: M1tsuha
LastEditTime: 2024-05-30 13:36:44
FilePath: \VSCProjects\Spark\test.py
Description: 

Copyright (c) 2024 by ${git_name_email}, All Rights Reserved. 
'''
import os
os.environ['SPARK_HOME'] = 'E:\\Configure\\Spark\\spark-3.4.0-bin-hadoop3-scala2.13'
import findspark
findspark.init()

from pyspark.sql.functions import col
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("FirstApp").getOrCreate()

data = spark.range(0, 10).select(col("id").cast("double"))

data.agg({'id': 'sum'}).show()

spark.stop()