'''
Author: M1tsuha
Date: 2024-05-31
LastEditors: M1tsuha
LastEditTime: 2024-06-01 23:21:01
Description: Script for basic operations on employee data in Spark.

Copyright (c) 2024 by M1tsuha, All Rights Reserved.
'''
import findspark
findspark.init()

from pyspark.sql import SparkSession

# 创建 Spark 会话
spark = SparkSession.builder.appName("EmployeeDataAnalysis").getOrCreate()

# 将数据加载到 DataFrame 中
data = spark.read.json('Spark/data/employee.json')

# 如果需要，填充缺失的年龄值或过滤掉没有年龄的记录

# 问题 1: 显示所有数据
print('-----------------------------------------------------')
print('问题 1: 显示所有数据')
data.show()

# 问题 2: 数据去重
print('-----------------------------------------------------')
print('问题 2: 数据去重')
data1 = data.dropDuplicates()
data1.show()

# 问题 3: 显示并移除 id
print('-----------------------------------------------------')
print('问题 3: 显示并移除 id')
data1.select('name', 'age').show()

# 问题 4: 显示年龄大于 30 的数据
print('-----------------------------------------------------')
print('问题 4: 显示年龄大于 30 的数据')
data1.where('age > 30').show()

# 问题 5: 按年龄分组
print('-----------------------------------------------------')
print('问题 5: 按年龄分组')
data1.groupBy('age').count().show()

# 问题 6: 按姓名升序排序
print('-----------------------------------------------------')
print('问题 6: 按姓名升序排序')
data1.orderBy(data['name'].asc()).show()

# 问题 7: 显示前 3 行数据
print('-----------------------------------------------------')
print('问题 7: 显示前 3 行数据')
data1.show(3)

# 问题 8: 选择名称作为用户名
print('-----------------------------------------------------')
print('问题 8: 选择名称作为用户名')
data1.selectExpr('name as username').show()

# 问题 9: 计算年龄的平均值
print('-----------------------------------------------------')
print('问题 9: 计算年龄的平均值')
mean = data1.describe('age').filter("summary = 'mean'").select('age').collect()[0].asDict()['age']
print('平均年龄是 ' + mean)

# 问题 10: 计算年龄的最小值
print('-----------------------------------------------------')
print('问题 10: 计算年龄的最小值')
min = data1.describe('age').filter("summary = 'min'").select('age').collect()[0].asDict()['age']
print('最小年龄是 ' + min)