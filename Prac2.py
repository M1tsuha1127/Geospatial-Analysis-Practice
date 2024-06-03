'''
Author: M1tsuha
Date: 2024-05-16 14:14:13
LastEditors: M1tsuha
LastEditTime: 2024-05-30 15:51:10
FilePath: \VSCProjects\Spark\Prac1.py
Description: 

Copyright (c) 2024 by WHURS, All Rights Reserved. 
'''

#coding=utf-8

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, sum, desc

# 创建SparkSession
spark = SparkSession.builder.appName('Data Processing for Beer Sales').getOrCreate()

# 读取CSV文件，确保文件路径正确
data = spark.read.option("header", "true").csv('Spark/data/实习2啤酒销量数据b.csv')

# 显示原始数据架构
data.printSchema()

print('\n-----------------第一问处理引号、逗号--------------------')
# 去除逗号并进行数据类型转换
for column in data.columns[3:]:  # 假定从第四列开始到最后都是数值类型的列
    data = data.withColumn(column, regexp_replace(col(column), ",", "").cast("integer"))

print('\n-----------------第二问剔除月销量为0的数据-------------------')
# 过滤整月销量非0的数据
data = data.filter((col("20181105实际销量") > 0) | (col("20181112实际销量") > 0) | (col("20181119实际销量") > 0))
data_count=data.count()
print(f"去除整月销量为 0 的数据共有：{data_count}")

print('\n-----------------第三问不同的啤酒类型-------------------')
# 统计不同类型的啤酒数量
beer_types_count = data.select("产品名称").distinct().count()
print(f"不同的啤酒类型共有：{beer_types_count}")

print('\n-----------------第四问销量最高的五种啤酒-------------------')
# 找出销量最高的五种啤酒
top_5_beers = data.groupBy("产品名称").agg(
    sum(col("20181105实际销量") + col("20181112实际销量") + col("20181119实际销量")).alias("total_sales")
).orderBy(desc("total_sales")).limit(5)
top_5_beers.show()

print('\n-----------------第五问同比去年增长最快的区域-------------------')
# 在去年销量大于500的区域中找出同比去年增长最快的区域
fastest_growing_area = data.filter(col("11月去年同期实际销量") > 500).withColumn(
    "growth_rate", 
    (col("20181105实际销量") + col("20181112实际销量") + col("20181119实际销量")-col("11月去年同期实际销量")) / col("11月去年同期实际销量")
).orderBy(desc("growth_rate")).limit(1)
top_1_growing_area = fastest_growing_area.select("DPUnit", (col("growth_rate")).alias("growth_rate_percent"))
top_1_growing_area.show()

print('\n-----------------第六问每种啤酒的11月份前三周销量-------------------')
# 统计每种啤酒的11月份前三周销量
beer_sales = data.groupBy("产品名称").agg(
    sum(col("20181105实际销量") + col("20181112实际销量") + col("20181119实际销量")).alias("total_sales")
)
beer_sales.show()

print('\n-----------------第七问销量最好的前三个区域的11月份前三周销量-------------------')
# 统计销量最好的前三个区域的11月份前三周销量
top_3_regions = data.groupBy("DPUnit").agg(
    sum(col("20181105实际销量") + col("20181112实际销量") + col("20181119实际销量")).alias("total_sales")
).orderBy(desc("total_sales")).limit(3)
top_3_regions.show()

# 停止SparkSession
spark.stop()
