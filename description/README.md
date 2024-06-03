### Practice.py 为代码文件

#### 运行环境为：

* Python版本：3.11.7；
* Java版本：1.8.0；
* Hadoop版本：3.2.2；
* Spark:spark-3.4.0-bin-hadoop3-scala2.13;
* 第三方库：pyspark、findspark、psutil、math；
* 系统环境：Windows10/11、VSCode的conda虚拟环境。
* 配置环境变量：SPARK_HOME、HADOOP_HOME、JAVA_HOME至系统变量。

#### 代码主要步骤为：

* 数据读取与预处理
* 数据清洗
* 时间序列分析
* 车辆瞬时速度计算
* 停留点判断
* 加减速分析
* 结果输出

### output.csv为输出文件

十列数据分别为row_num（行号也即点序）、date（日期）、time（时间）、latitude（纬度）、longitude（经度）、speed（速度）、acceleration（加速度）、is_accelerating（加速状态）、is_decelerating（减速状态）、is_stop（停留点标志）。

### 实习报告有doc和pdf两个版本

### data文件夹中是实验所用数据

是一个plt文件，前六行是有关该点集数据的一些说明。

### extra文件夹中是附录中提到的华中杯题目与论文
