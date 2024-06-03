import findspark
findspark.init()

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()

def print_data(data):
    v = data.asDict()
    # 确保所有键都存在
    print('id:' + str(v.get('id', 'NA')) + ',name:' + v.get('name', 'NA') + ',age:' + str(v.get('age', 'NA')))

# 创建rdd对象
data_rdd = spark.sparkContext.textFile('Spark/data/employee.txt')

# 格式转换，并确保数据类型正确
data_rdd = data_rdd.map(lambda line: line.split(",")).\
        map(lambda x: Row(id=int(x[0]), name=x[1], age=int(x[2])))

# 转换为DataFrame
data_df = spark.createDataFrame(data_rdd)

# 输出
data_df.foreach(print_data)  # 应用 print_result 函数打印每行数据
