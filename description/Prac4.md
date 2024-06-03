### 数据读取和清洗

##### 定义数据读取和清洗函数：

`load_and_clean_data(filepath, has_dot=False) `函数用于读取 CSV 文件，可选地处理测试数据集中行尾的点（.）。
raw_data 使用 spark.read.csv 读取数据，可通过 inferSchema=True 自动推断字段类型，header=False 表明数据文件没有表头。
如果 has_dot=True，使用 expr() 函数移除 _c14 列（即收入标签列）末尾的点。
使用 select 和 cast 函数显式转换数据类型，并为列重命名，确保数据集的一致性和准确性。

### 数据预处理和模型训练

##### 加载并清洗数据：

分别加载训练和测试数据，对测试数据应用点的移除。

##### 查看数据 schema 和部分数据：

打印数据模式和部分数据，验证数据加载和清洗的正确性。

##### 数据预处理：

使用 StringIndexer 将 income 字符串标签转换为数值索引,使用 VectorAssembler 将多个数值特征组合成单个特征向量。

### 训练和评估决策树模型

##### 决策树模型：

定义 DecisionTreeClassifier，指定标签和特征列。
使用 Pipeline 封装数据预处理和模型训练步骤，简化训练过程。

##### 模型训练和预测：

在训练数据上训练模型。
在测试数据上应用模型进行预测。

##### 模型评估：

使用 MulticlassClassificationEvaluator 评估模型的准确性。
打印模型的准确率。

## 模型最终准确率为0.82
