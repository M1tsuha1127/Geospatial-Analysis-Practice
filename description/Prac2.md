### 初始化和数据加载

**环境设置和SparkSession创建** ：

* 初始化 findspark，使 Python 能识别 Spark 安装路径。
* 创建一个 `SparkSession` 对象，是使用 Spark SQL 进行数据操作的主入口。

**读取数据** ：

* 使用 `spark.read.option("header", "true").csv(...)` 读取 CSV 文件。这里假设 CSV 文件的第一行包含列标题。

**显示数据架构** ：

* 使用 `data.printSchema()` 打印数据的结构，了解各列的名称和类型。

### 数据清洗与转换

###### **1.处理引号和逗号** ：

* 遍历数据的数值列（假设从第四列开始），使用 `regexp_replace` 函数去除数字中的逗号，并将其转换为整数类型。这有助于后续的数值计算。

  ```
  for column in data.columns[3:]:  # 假定从第四列开始到最后都是数值类型的列
      data = data.withColumn(column, regexp_replace(col(column), ",", "").cast("integer"))
  ```

###### **2.剔除月销量为0的数据** ：

* 使用 `filter` 方法结合逻辑或操作过滤掉所有指定列（三个特定日期的销量）均为0的记录。

  ```
  # 过滤整月销量非0的数据
  data = data.filter((col("20181105实际销量") > 0) | (col("20181112实际销量") > 0) | (col("20181119实际销量") > 0))
  data_count=data.count()
  ```

### 数据分析

###### **3.不同啤酒类型的统计** ：统计结果为34种

* 计算并打印不同啤酒类型的数量，使用 `distinct().count()` 方法统计 "产品名称" 列的不同值。

  ```
  # 统计不同类型的啤酒数量
  beer_types_count = data.select("产品名称").distinct().count()
  print(f"不同的啤酒类型共有：{beer_types_count}")
  ```

###### **4.销量最高的五种啤酒** ：

* 对每种啤酒的三周销量进行汇总，并按销量降序排列，展示销量最高的前五种。

  ```
  # 找出销量最高的五种啤酒
  top_5_beers = data.groupBy("产品名称").agg(
      sum(col("20181105实际销量") + col("20181112实际销量") + col("20181119实际销量")).alias("total_sales")
  ).orderBy(desc("total_sales")).limit(5)
  top_5_beers.show()
  ```

###### **5.同比去年增长最快的区域** ：结果为驻马店郊县 同比增长436.36%

* 在去年销量大于500的基础上，计算各区域今年三周销量与去年同期的增长率，找出增长最快的区域。

  ```
  # 在去年销量大于500的区域中找出同比去年增长最快的区域
  fastest_growing_area = data.filter(col("11月去年同期实际销量") > 500).withColumn(
      "growth_rate", 
      (col("20181105实际销量") + col("20181112实际销量") + col("20181119实际销量")-col("11月去年同期实际销量")) / col("11月去年同期实际销量")
  ).orderBy(desc("growth_rate")).limit(1)
  top_1_growing_area = fastest_growing_area.select("DPUnit", (col("growth_rate")).alias("growth_rate_percent"))
  top_1_growing_area.show()

  ```

###### **6.每种啤酒的11月份前三周销量统计** ：

* 统计每种啤酒在11月份前三周的总销量。

  ```
  # 统计每种啤酒的11月份前三周销量
  beer_sales = data.groupBy("产品名称").agg(
      sum(col("20181105实际销量") + col("20181112实际销量") + col("20181119实际销量")).alias("total_sales")
  )
  beer_sales.show()
  ```

###### **7.销量最好的前三个区域的11月份前三周销量** ：分别为信阳市区 豫南nl 信阳东片区

* 找出销量最好的前三个区域，并统计它们在11月份前三周的销量。

  ```
  # 统计销量最好的前三个区域的11月份前三周销量
  top_3_regions = data.groupBy("DPUnit").agg(
      sum(col("20181105实际销量") + col("20181112实际销量") + col("20181119实际销量")).alias("total_sales")
  ).orderBy(desc("total_sales")).limit(3)
  top_3_regions.show()
  ```
