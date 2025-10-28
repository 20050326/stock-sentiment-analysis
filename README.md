# 股票情感分析 MapReduce 项目

## 项目概述

本项目使用 Hadoop MapReduce 框架分析股票新闻数据，统计正面和负面情感新闻标题中的高频词汇。通过两个 MapReduce 作业实现完整的数据处理流程，最终输出每个情感类别中出现频率最高的前100个单词。

## 设计思路

### 1. 双阶段 MapReduce 架构

- **第一个作业（词频统计）**：对原始新闻数据进行分词、过滤和情感分类计数
- **第二个作业（排序筛选）**：对每个情感类别的单词按频率排序，取前100个高频词

### 2. 数据处理流程

```
原始新闻数据 → 文本清洗 → 分词 → 停用词过滤 → 情感分类计数 → 排序 → 前100高频词
```

### 3. 技术特性

- **文本预处理**：忽略大小写、去除标点符号和数字
- **停用词过滤**：使用分布式缓存加载停用词表
- **情感分类**：基于数据集中提供的情感标签（1=正面，-1=负面）
- **分布式计算**：利用 Hadoop MapReduce 进行大规模数据处理

## 项目结构

```
stock-sentiment-analysis/
├── src/main/java/StockSentimentAnalysis/
│   ├── WordCountMapper.java          # 第一个作业的Mapper
│   ├── WordCountReducer.java         # 第一个作业的Reducer
│   ├── TopWordsMapper.java           # 第二个作业的Mapper  
│   ├── TopWordsReducer.java          # 第二个作业的Reducer
│   ├── SentimentAnalysisDriver.java  # 主驱动程序
│   └── SimpleSentimentAnalysis.java  # 简化版本驱动
├── target/
│   └── stock-sentiment-analysis-1.0.jar  # 可执行JAR文件
├── output/
│   └── final_results.txt             # 最终输出结果
├── pom.xml                           # Maven配置文件
└── README.md                         # 项目说明文档
```

## 环境要求

- Hadoop 3.3.6 （单机伪分布式）
- Java 8
- Maven 3.6+

## 数据格式

### 输入数据格式
```
新闻标题,情感标签
Company reports strong earnings growth,1
Stock prices plummet due to market crash,-1
...
```

### 输出数据格式
```
情感_单词<TAB>频次
1_company<TAB>156
-1_stock<TAB>89
...
```

## 运行方式

### 1. 准备数据
```bash
# 上传数据到HDFS
hdfs dfs -mkdir -p /user/hduser/input
hdfs dfs -mkdir -p /user/hduser/stopwords
hdfs dfs -put stock_data.csv /user/hduser/input/
hdfs dfs -put stop-word-list.txt /user/hduser/stopwords/
```

### 2. 编译项目
```bash
mvn clean package -Dmaven.test.skip=true
```

### 3. 运行MapReduce作业
```bash
hadoop jar target/stock-sentiment-analysis-1.0.jar \
  StockSentimentAnalysis.SentimentAnalysisDriver \
  /user/hduser/input/stock_data.csv \
  /user/hduser/stopwords/stop-word-list.txt \
  /user/hduser/output
```

### 4. 查看结果
```bash
# 查看最终结果
hdfs dfs -cat /user/hduser/output/part-r-00000

# 分别查看正面和负面情感结果
hdfs dfs -cat /user/hduser/output/part-r-00000 | grep "^1_"
hdfs dfs -cat /user/hduser/output/part-r-00000 | grep "^-1_"
```

## 程序运行结果

### 作业执行统计
- **输入数据**：6,090条新闻记录
- **第一个作业输出**：16,200个情感-单词频次映射
- **第二个作业输出**：200个高频词（正面100个 + 负面100个）
- **数据处理效率**：从16,200个单词中筛选出200个最高频词

### 典型输出示例
```
1_company   156
1_growth    134
1_earnings  128
1_strong    115
-1_stock    89
-1_prices   76
-1_market   68
-1_fall     59
```

### 常见问题
1. **JAR文件不存在**
   ```bash
   mvn clean package -Dmaven.test.skip=true
   ```

2. **输出目录已存在**
   ```bash
   hdfs dfs -rm -r /user/hduser/output
   ```

3. **类型不匹配错误**
   - 检查Mapper和Reducer的输出类型是否一致
   - 确认驱动程序中设置了正确的Map输出类型

4. **停用词文件不存在**
   ```bash
   hdfs dfs -put stop-word-list.txt /user/hduser/stopwords/
   ```
   

本项目仅用于教学和研究目的。
