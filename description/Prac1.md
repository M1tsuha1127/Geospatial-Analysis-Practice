## **环境配置** ：

import os: 导入 Python 的 os 模块，用于与操作系统交互。

`os.environ['SPARK_HOME'] = 'E:\\Configure\\Spark\\spark-3.4.0-bin-hadoop3-scala2.13'`: 设置环境变量 `SPARK_HOME`，指向本地安装的 Spark 目录。这是配置 PySpark 运行所必需的。

`import findspark`: 导入 findspark 模块，它用于在 Python 环境中定位和初始化 Spark。

`findspark.init()`: 初始化 findspark，这样 Python 就可以找到 Spark 的安装路径，并使用其 API。

## **创建 Spark 会话** ：

`from pyspark.sql import SparkSession`: 从 pyspark.sql 模块导入 SparkSession 类，它是与 Spark 交互的主入口点。

`spark = SparkSession.builder.master("local[*]").appName("FirstApp").getOrCreate()`: 创建一个 SparkSession 对象。这里设置 master 为 `local[*]` 表示在本地使用所有可用的核心。`appName("FirstApp")` 设置应用程序的名称，`getOrCreate()` 方法会获取一个已存在的 SparkSession 或者创建一个新的。

## **数据操作** ：

`data = spark.range(0, 10).select(col("id").cast("double"))`: 使用 `spark.range(0, 10)` 创建一个 DataFrame，该 DataFrame 包含一个名为 `id` 的列，其中包含从 0 到 9 的整数。然后通过 `.select(col("id").cast("double"))` 将 `id` 列的数据类型转换为双精度浮点数。

`data.agg({'id': 'sum'}).show()`: 对 `data` DataFrame 使用聚合操作，计算 `id` 列的总和，并使用 `show()` 方法显示结果。

## **结束会话** ：

`spark.stop()`: 结束 SparkSession，释放与之关联的资源。
