'''
Author: M1tsuha
Date: 2024-06-01 23:21:46
LastEditors: M1tsuha
LastEditTime: 2024-06-02 01:16:16
FilePath: \VSCProjects\Spark\Prac4.py
Description: 

Copyright (c) 2024 by ${git_name_email}, All Rights Reserved. 
'''
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, expr
from pyspark.sql.types import IntegerType, StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# 初始化 Spark Session
spark = SparkSession.builder.appName("Adult Income Prediction").getOrCreate()

# 定义数据读取和清洗函数
def load_and_clean_data(filepath, has_dot=False):
    # 读取 CSV 文件，关闭 schema 自动推断
    raw_data = spark.read.csv(filepath, inferSchema=True, header=False)
    
    # 如果数据来自测试集，移除每行末尾的点
    if has_dot:
        raw_data = raw_data.withColumn("_c14", expr("substring(_c14, 1, length(_c14)-1)"))

    # 显式定义数据转换类型
    data = raw_data.select(
        col("_c0").cast(IntegerType()).alias("age"),
        col("_c1").cast(StringType()).alias("workclass"),
        col("_c4").cast(IntegerType()).alias("education_num"),
        col("_c10").cast(IntegerType()).alias("capital_gain"),
        col("_c11").cast(IntegerType()).alias("capital_loss"),
        col("_c12").cast(IntegerType()).alias("hours_per_week"),
        col("_c14").cast(StringType()).alias("income")
    )

    return data

# 文件路径需要根据您的实际路径进行修改
train_path = "E:/Desktop/Code/VSCProjects/Spark/data/adult.data"
test_path = "E:/Desktop/Code/VSCProjects/Spark/data/adult.test"

# 加载并清洗数据
training_data = load_and_clean_data(train_path)
test_data = load_and_clean_data(test_path, has_dot=True)

# 查看数据schema和部分数据
training_data.printSchema()
training_data.show(5)
test_data.show(5)

# 数据预处理：转换字符串标签为索引
label_indexer = StringIndexer(inputCol="income", outputCol="label").fit(training_data)
assembler = VectorAssembler(inputCols=["age", "education_num", "capital_gain", "capital_loss", "hours_per_week"], outputCol="features")

# 训练决策树模型
dt = DecisionTreeClassifier(labelCol="label", featuresCol="features")
pipeline = Pipeline(stages=[label_indexer, assembler, dt])

# 训练模型
model = pipeline.fit(training_data)

# 预测和评估
predictions = model.transform(test_data)
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print(f"Model accuracy: {accuracy:.2f}")

# 结束 Spark Session
spark.stop()
