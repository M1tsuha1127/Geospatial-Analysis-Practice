### 任务一：为employee.json创建 DataFrame，并写出 Python语句完成下列操作：


##### 环境配置与数据加载

###### **环境初始化** ：

使用 `findspark.init()` 初始化 Spark，使 Python 能够找到 Spark 的安装位置，并使用其 API。

###### **创建 SparkSession** ：

创建一个 `SparkSession`，这是 Spark 2.0 以后用于执行所有 DataFrame 和 SQL 操作的入口。这里，会话被命名为 "EmployeeDataAnalysis"。

###### **数据加载** ：

使用 `spark.read.json('Spark/data/employee.json')` 读取 JSON 格式的文件，并将数据加载到一个 DataFrame 中。这假设 JSON 文件结构符合 DataFrame 的要求，即文件中的每个对象都有一致的字段。

##### 数据处理和查询

###### **1.显示所有数据** ：

使用 `data.show()` 输出 DataFrame 中的所有数据，以便于查看和分析。

###### **2.数据去重** ：

使用 `data.dropDuplicates()` 方法移除 DataFrame 中的重复行，创建一个新的 DataFrame `data1`。

###### **3.显示并移除 ID** ：

使用 `data1.select('name', 'age')` 选择 `name` 和 `age` 两列，并显示结果。这实现了从视觉上“移除”了 `id` 列。

###### **4.显示年龄大于 30 的数据** ：

使用 `data1.where('age > 30')` 筛选年龄大于 30 的记录，并显示这些数据。

###### **5.按年龄分组** ：

使用 `data1.groupBy('age').count()` 按年龄进行分组，并计算每个年龄组的记录数，显示分组结果。

###### **6.按姓名升序排序** ：

使用 `data1.orderBy(data['name'].asc())` 按姓名进行升序排序，并显示排序后的数据。

###### **7.显示前 3 行数据** ：

使用 `data1.show(3)` 显示 DataFrame 的前三行数据。

###### **8.选择名称作为用户名** ：

使用 `data1.selectExpr('name as username')` 选择 `name` 列，并将其重命名为 `username`，然后显示结果。

###### **9.计算年龄的平均值和最小值** ：

使用 `data1.describe('age')` 生成年龄的统计摘要，包括平均值和最小值。

使用 `filter("summary = 'mean'")` 和 `filter("summary = 'min'")` 分别筛选出平均值和最小值，然后通过 `.collect()` 将结果收集为本地对象，并通过 `.asDict()['age']` 获取具体的值。

打印计算出的平均年龄和最小年龄。


### 任务二：请 将数据复制保存到系统中，命名为 employee.txt，实现从 RDD转换得到 DataFrame，并按 “id:1,name:Ella,age:36”的格式打印出 DataFrame的所有数据 。

##### 初始化和 SparkSession 创建

###### **环境初始化** ：

使用 `findspark.init()` 初始化 Spark，使 Python 能找到 Spark 的安装路径并使用其 API。

###### **SparkSession 创建** ：

创建一个 `SparkSession` 对象，这是 Spark 2.0 以后用于执行所有 DataFrame 和 SQL 操作的入口。这里没有特别配置，使用默认的 `SparkConf()`。

##### 数据处理

###### **定义数据打印函数 ：**

`print_data` 函数接受一个 `Row` 对象，转换为字典，并安全地打印出每个字段的值。使用 `.get()` 方法从字典中提取值，如果指定键不存在，返回 `"NA"`。

###### **数据读取** ：

使用 `spark.sparkContext.textFile('Spark/data/employee.txt')` 读取文本文件。假设该文本文件包含逗号分隔的数据，例如 `id,name,age`。

###### **数据转换** ：

`data_rdd` 是一个 RDD 对象，由文本文件的每一行组成。

通过 `.map(lambda line: line.split(","))` 将每行文本按逗号分割成列表。

再通过 `.map(lambda x: Row(id=int(x[0]), name=x[1], age=int(x[2])))` 将列表转换为 `Row` 对象，同时将 `id` 和 `age` 转换为整数类型。这是为了确保数据类型的正确性，有利于后续处理。

##### DataFrame 创建和数据输出

###### **DataFrame 创建** ：

使用 `spark.createDataFrame(data_rdd)` 将 RDD 转换为 DataFrame。`createDataFrame` 方法可以接受一个 RDD，并根据 RDD 中的 `Row` 对象推断出 DataFrame 的结构（schema）。

###### **数据输出** ：

使用 `data_df.foreach(print_data)` 对 DataFrame 中的每行数据应用 `print_data` 函数。这里使用 `foreach` 方法，它适用于对 DataFrame 中的每个元素执行指定操作，但需要注意，`foreach` 在分布式环境中执行，不保证执行顺序。


### 任务三：在 MySQL数据库中新建数据库 testspark，再创建表 employee；配置 Spark通过 JDBC连接数据库 MySQL，编程实现利用 DataFrame插入如下表所示的两行数据到 MySQL中，最后打印出 age的最大值和 age的总和。

##### 创建数据库

`CREATE DATABASE testspark;`
这条命令创建了一个名为 testspark 的新数据库。如果数据库已经存在，则此命令可能会失败，除非配置了相应的错误处理或覆盖选项。

##### 选择数据库

`USE testspark;`
这条命令指定后续的所有数据库操作都将在 testspark 数据库中进行。这是在创建表格和插入数据之前必须执行的步骤，确保操作在正确的数据库中执行。

##### 创建表

`CREATE TABLE employee (
id INT PRIMARY KEY,`：定义一个名为 id 的列，数据类型为整数（INT）。这个列被指定为主键（PRIMARY KEY），意味着该列的每个值必须唯一，且不允许为 NULL。
`name VARCHAR(50),`：定义一个名为 name 的列，数据类型为可变长度字符串（VARCHAR），最大长度为 50 个字符。
`gender CHAR(1),`：定义一个名为 gender 的列，数据类型为固定长度的字符（CHAR），长度为 1。这通常用于存储性别信息，如 'M' 或 'F'。
`age INT`：定义一个名为 age 的列，数据类型为整数。
`);`：SQL 语句结束符。

##### 插入数据

`INSERT INTO employee (id, name, gender, age) VALUES (1, 'Alice', 'F', 22),`：向 employee 表中插入一行数据。此行为 id 为 1，name 为 'Alice'，gender 为 'F'，age 为 22。
`(2, 'John', 'M', 25);`：向 employee 表中再插入一行数据。此行为 id 为 2，name 为 'John'，gender 为 'M'，age 为 25。
这些命令共同完成了在数据库中创建一个新表 employee 并填充了一些初始数据的过程。把这些代码写至sql脚本中，在mysql server下直接运行该脚本。

##### 构建数据模型和加载数据

###### 定义数据模式（schema）：

使用 StructType 和 StructField 定义了一个 schema，这个 schema 描述了 DataFrame 的结构，包括列名和数据类型。这里定义的字段有 id, name, gender, age。

###### 创建 DataFrame：

将原始数据（以列表形式提供）转换成 DataFrame。数据被定义为包含元组的列表，每个元组对应一行数据，字段顺序与 schema 定义一致。
spark.createDataFrame(data, schema)：利用提供的数据和 schema 创建 DataFrame。

##### 数据库连接和数据写入

###### 设置数据库连接属性：

定义一个字典，包含连接 MySQL 数据库所需的属性，如用户名 (user), 密码 (password), 和 JDBC 驱动 (driver)。

###### 数据写入 MySQL：

使用 DataFrame.write.jdbc() 方法将数据写入 MySQL。需要提供 JDBC URL ("jdbc:mysql://localhost:3306/testspark"), 表名 ("employee"), 和写入模式 ("append"：在现有表中追加数据)。

##### 读取数据库数据

###### 从 MySQL 读取数据：

`spark.read.jdbc("jdbc:mysql://localhost:3306/testspark", "employee", properties=properties)`：这行代码使用 PySpark 的 JDBC 接口从 MySQL 数据库读取数据。
"jdbc:mysql://localhost:3306/testspark"：这是 JDBC 连接字符串，指定了数据库的位置（本例中为本地主机上的 testspark 数据库）。
"employee"：指定从哪个表读取数据（此处为 employee 表）。
properties=properties：提供了一个包含数据库连接信息的字典（如用户名、密码和使用的 JDBC 驱动）。
执行此操作后，读取的数据存储在 DataFrame df 中。

##### **数据聚合和计算**

###### 聚合操作计算最大年龄和年龄总和：

`df.agg({"age": "max"})` 和 `df.agg({"age": "sum"})：`这两个聚合操作分别计算 age 列的最大值和总和。
agg() 方法接收一个字典参数，其中键为要聚合的列名（此例中为 "age"），值为聚合函数（"max" 和 "sum"）。
collect()[0][0]：collect() 方法将 DataFrame 转换为本地 Python 对象（此处为列表）。因为 agg() 返回的结果只有一个值，所以通过 [0][0] 访问这个值。
