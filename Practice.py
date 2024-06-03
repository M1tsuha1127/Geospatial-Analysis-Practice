'''
Author: M1tsuha
Date: 2024-06-02 22:10:58
LastEditors: M1tsuha
LastEditTime: 2024-06-03 23:53:11
FilePath: \VSCProjects\Spark\Practice.py
Description: 

Copyright (c) 2024 by ${git_name_email}, All Rights Reserved. 
'''
#coding=utf-8
import os
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
from pyspark.sql import Row
from pyspark.sql.functions import col, radians, lag, lead, udf, when, row_number, count
from pyspark.sql.window import Window
import math
from math import radians, sin, cos, sqrt, atan2

# Initialize Spark Session
spark = SparkSession.builder.appName("Trajectory Analysis").getOrCreate()

# Define schema
schema = StructType([
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("unused", IntegerType(), True),
    StructField("altitude", DoubleType(), True),
    StructField("date_numeric", DoubleType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True)
])

# Read data and skip first 6 rows
rdd = spark.sparkContext.textFile('Spark/data/20090415134400.plt')
filtered_rdd = rdd.zipWithIndex().filter(lambda x: x[1] > 5).map(lambda x: x[0])
row_rdd = filtered_rdd.map(lambda x: x.split(",")).map(lambda p: Row(
    latitude=float(p[0]),
    longitude=float(p[1]),
    unused=int(p[2]),
    altitude=float(p[3]),
    date_numeric=float(p[4]),
    date=p[5],
    time=p[6]
))

# Create DataFrame
data = spark.createDataFrame(row_rdd, schema)
data = data.filter(data.altitude != -777)  # Filter out invalid altitude values

# Define Haversine UDF to calculate distances
def haversine(lon1, lat1, lon2, lat2):
    if None in (lon1, lat1, lon2, lat2):
        return None
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    r = 6371  # Radius of Earth in kilometers
    return c * r * 1000  # Return distance in meters

haversine_udf = udf(haversine, DoubleType())

# Prepare analysis window and calculate row numbers
windowSpec = Window.orderBy("date_numeric")

windowSpecRows = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
data = data.withColumn("row_num", row_number().over(windowSpec))
data = data.withColumn("total_rows", count("date_numeric").over(windowSpecRows))

# Calculate distances and speeds, setting speed to zero for the first and last rows directly
data = data.withColumn("prev_latitude", lag("latitude").over(windowSpec))
data = data.withColumn("prev_longitude", lag("longitude").over(windowSpec))
data = data.withColumn("prev_time", lag("date_numeric").over(windowSpec))
data = data.withColumn("distance", haversine_udf(col("longitude"), col("latitude"), col("prev_longitude"), col("prev_latitude")))
data = data.withColumn("time_diff", (col("date_numeric") - col("prev_time")) * 24 * 3600)


data = data.withColumn("speed",
                        when((col("row_num") == 1) | (col("row_num") == col("total_rows")) ,0)
                       .otherwise(col("distance") / col("time_diff")))

# Calculate acceleration
data = data.withColumn("acceleration", (lead("speed").over(windowSpec) - col("speed")) / lead("time_diff").over(windowSpec))

# Mark acceleration and deceleration
data = data.withColumn("is_accelerating", when((col("acceleration") > 0.1) & (lag("acceleration").over(windowSpec) > 0.1), True).otherwise(False))
data = data.withColumn("is_decelerating", when((col("acceleration") < -0.1) & (lag("acceleration").over(windowSpec) < -0.1), True).otherwise(False))

# Mark stopping points
data = data.withColumn("is_stop", when((col("speed") < 0.5)&(lag("speed").over(windowSpec)<0.5), True).otherwise(False))

# Clean up added columns for row numbers
data = data.drop("total_rows")

# Display results
data.select("row_num","date", "time", "latitude", "longitude", "speed", "acceleration", "is_accelerating", "is_decelerating", "is_stop").show(200)

# Stop Spark Session
spark.stop()
