# Spark 示例代码库

本文档提供 Apache Spark 1.6.3 的各种场景示例代码。

## 目录

- [基础示例](#基础示例)
- [RDD 操作](#rdd-操作)
- [Spark SQL](#spark-sql)
- [Spark Streaming](#spark-streaming)
- [数据读写](#数据读写)
- [高级特性](#高级特性)

---

## 基础示例

### WordCount

**Scala 版本**：

```scala
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("hdfs://path/to/file.txt")
    val counts = textFile
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.saveAsTextFile("hdfs://path/to/output")
    sc.stop()
  }
}
```

**Python 版本**：

```python
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("WordCount")
sc = SparkContext(conf=conf)

textFile = sc.textFile("hdfs://path/to/file.txt")
counts = textFile \
    .flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)

counts.saveAsTextFile("hdfs://path/to/output")
sc.stop()
```

### 计算 Pi

```scala
import scala.math.random
import org.apache.spark.{SparkConf, SparkContext}

object SparkPi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi")
    val sc = new SparkContext(conf)

    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt

    val count = sc.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)

    println(s"Pi is roughly ${4.0 * count / (n - 1)}")
    sc.stop()
  }
}
```

---

## RDD 操作

### Transformation 操作

```scala
val rdd = sc.parallelize(1 to 100)

// map：转换每个元素
val mapped = rdd.map(_ * 2)

// filter：过滤元素
val filtered = rdd.filter(_ % 2 == 0)

// flatMap：扁平化映射
val flatMapped = rdd.flatMap(x => List(x, x * 2))

// mapPartitions：按分区处理
val partitioned = rdd.mapPartitions { partition =>
  val conn = createConnection()
  partition.map(x => process(conn, x))
}

// distinct：去重
val distinct = rdd.distinct()

// union：合并
val rdd2 = sc.parallelize(101 to 200)
val union = rdd.union(rdd2)

// intersection：交集
val intersection = rdd.intersection(rdd2)

// subtract：差集
val subtract = rdd.subtract(rdd2)
```

### Action 操作

```scala
val rdd = sc.parallelize(1 to 100)

// count：计数
val count = rdd.count()

// collect：收集到 Driver
val collected = rdd.collect()

// take：取前 N 个
val taken = rdd.take(10)

// first：取第一个
val first = rdd.first()

// reduce：聚合
val sum = rdd.reduce(_ + _)

// fold：带初始值的聚合
val folded = rdd.fold(0)(_ + _)

// aggregate：更灵活的聚合
val avg = rdd.aggregate((0, 0))(
  (acc, value) => (acc._1 + value, acc._2 + 1),
  (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
)

// foreach：遍历
rdd.foreach(println)

// saveAsTextFile：保存为文本文件
rdd.saveAsTextFile("hdfs://path/to/output")
```

### Pair RDD 操作

```scala
val pairRDD = sc.parallelize(List(("a", 1), ("b", 2), ("a", 3)))

// reduceByKey：按 Key 聚合
val reduced = pairRDD.reduceByKey(_ + _)

// groupByKey：按 Key 分组
val grouped = pairRDD.groupByKey()

// aggregateByKey：按 Key 聚合（更灵活）
val aggregated = pairRDD.aggregateByKey(0)(
  (acc, value) => acc + value,
  (acc1, acc2) => acc1 + acc2
)

// sortByKey：按 Key 排序
val sorted = pairRDD.sortByKey()

// join：内连接
val rdd2 = sc.parallelize(List(("a", 10), ("b", 20)))
val joined = pairRDD.join(rdd2)

// leftOuterJoin：左外连接
val leftJoined = pairRDD.leftOuterJoin(rdd2)

// cogroup：协同分组
val cogrouped = pairRDD.cogroup(rdd2)
```

---

## Spark SQL

### DataFrame 基础操作

```scala
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
import sqlContext.implicits._

// 从 JSON 创建 DataFrame
val df = sqlContext.read.json("people.json")

// 显示数据
df.show()

// 打印 Schema
df.printSchema()

// 选择列
df.select("name", "age").show()

// 过滤
df.filter($"age" > 21).show()

// 分组聚合
df.groupBy("age").count().show()

// 排序
df.orderBy($"age".desc).show()

// 重命名列
df.select($"name".as("username"), $"age").show()

// 添加列
df.withColumn("age_plus_10", $"age" + 10).show()

// 删除列
df.drop("age").show()
```

### SQL 查询

```scala
// 注册临时表
df.registerTempTable("people")

// 执行 SQL
val result = sqlContext.sql("SELECT name, age FROM people WHERE age > 21")
result.show()

// 复杂查询
val complexQuery = sqlContext.sql("""
  SELECT
    age,
    COUNT(*) as count,
    AVG(salary) as avg_salary
  FROM people
  WHERE age > 18
  GROUP BY age
  HAVING count > 10
  ORDER BY age DESC
""")
```

### Join 操作

```scala
val employees = sqlContext.read.json("employees.json")
val departments = sqlContext.read.json("departments.json")

// Inner Join
val joined = employees.join(departments, "dept_id")

// Left Outer Join
val leftJoined = employees.join(departments, Seq("dept_id"), "left_outer")

// Broadcast Join
import org.apache.spark.sql.functions.broadcast
val broadcastJoined = employees.join(broadcast(departments), "dept_id")
```

### UDF (User Defined Function)

```scala
import org.apache.spark.sql.functions.udf

// 定义 UDF
val upper = udf((s: String) => s.toUpperCase)

// 使用 UDF
df.select(upper($"name").as("upper_name")).show()

// 注册 UDF 用于 SQL
sqlContext.udf.register("upper", (s: String) => s.toUpperCase)
sqlContext.sql("SELECT upper(name) FROM people").show()
```

### 数据源

```scala
// Parquet
val parquetDF = sqlContext.read.parquet("data.parquet")
df.write.parquet("output.parquet")

// JSON
val jsonDF = sqlContext.read.json("data.json")
df.write.json("output.json")

// CSV (需要 spark-csv 包)
val csvDF = sqlContext.read
  .format("com.databricks.spark.csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("data.csv")

// JDBC
val jdbcDF = sqlContext.read
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/db")
  .option("dbtable", "table_name")
  .option("user", "username")
  .option("password", "password")
  .load()
```

---

## Spark Streaming

### 基础示例

```scala
import org.apache.spark.streaming.{Seconds, StreamingContext}

val ssc = new StreamingContext(sc, Seconds(5))

// 从 Socket 读取
val lines = ssc.socketTextStream("localhost", 9999)

// 处理数据
val words = lines.flatMap(_.split(" "))
val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

// 输出结果
wordCounts.print()

// 启动
ssc.start()
ssc.awaitTermination()
```

### Kafka 集成

```scala
import org.apache.spark.streaming.kafka._

// Direct 方式（推荐）
val kafkaParams = Map[String, String](
  "metadata.broker.list" -> "broker1:9092,broker2:9092",
  "auto.offset.reset" -> "smallest"
)

val topics = Set("topic1", "topic2")

val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
  ssc, kafkaParams, topics
)

// 处理数据
stream.foreachRDD { rdd =>
  rdd.foreach { case (key, value) =>
    println(s"Key: $key, Value: $value")
  }
}
```

### 窗口操作

```scala
// 窗口长度 30 秒，滑动间隔 10 秒
val windowedCounts = words
  .map(x => (x, 1))
  .reduceByKeyAndWindow(
    (a: Int, b: Int) => a + b,
    Seconds(30),
    Seconds(10)
  )

windowedCounts.print()
```

### 状态管理

```scala
// updateStateByKey
val runningCounts = words.map(x => (x, 1)).updateStateByKey { (values: Seq[Int], state: Option[Int]) =>
  val currentCount = values.sum
  val previousCount = state.getOrElse(0)
  Some(currentCount + previousCount)
}

// 需要设置 Checkpoint
ssc.checkpoint("hdfs://path/to/checkpoint")
```

---

## 数据读写

### HDFS

```scala
// 读取文本文件
val textRDD = sc.textFile("hdfs://namenode:8021/path/to/file.txt")

// 读取多个文件
val multiRDD = sc.textFile("hdfs://namenode:8021/path/to/*.txt")

// 读取目录
val dirRDD = sc.textFile("hdfs://namenode:8021/path/to/dir")

// 写入文本文件
rdd.saveAsTextFile("hdfs://namenode:8021/path/to/output")

// 读取 SequenceFile
val seqRDD = sc.sequenceFile[String, Int]("hdfs://path/to/seq")

// 写入 SequenceFile
pairRDD.saveAsSequenceFile("hdfs://path/to/output")
```

### 本地文件系统

```scala
// 读取
val localRDD = sc.textFile("file:///path/to/file.txt")

// 写入
rdd.saveAsTextFile("file:///path/to/output")
```

### S3

```scala
// 配置 S3 访问
sc.hadoopConfiguration.set("fs.s3a.access.key", "ACCESS_KEY")
sc.hadoopConfiguration.set("fs.s3a.secret.key", "SECRET_KEY")

// 读取
val s3RDD = sc.textFile("s3a://bucket/path/to/file.txt")

// 写入
rdd.saveAsTextFile("s3a://bucket/path/to/output")
```

---

## 高级特性

### 广播变量

```scala
// 创建广播变量
val broadcastVar = sc.broadcast(Array(1, 2, 3))

// 使用广播变量
val result = rdd.map(x => x * broadcastVar.value(0))

// 销毁广播变量
broadcastVar.unpersist()
```

### 累加器

```scala
// 创建累加器
val accum = sc.accumulator(0, "My Accumulator")

// 使用累加器
rdd.foreach(x => accum += x)

// 获取累加器值
println(accum.value)
```

### 自定义分区器

```scala
import org.apache.spark.Partitioner

class CustomPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    key match {
      case k: String => k.hashCode % numPartitions
      case _ => 0
    }
  }
}

// 使用自定义分区器
val partitioned = pairRDD.partitionBy(new CustomPartitioner(10))
```

### 缓存和持久化

```scala
import org.apache.spark.storage.StorageLevel

// 缓存到内存
rdd.cache()

// 持久化到内存和磁盘
rdd.persist(StorageLevel.MEMORY_AND_DISK)

// 持久化到内存（序列化）
rdd.persist(StorageLevel.MEMORY_ONLY_SER)

// 取消持久化
rdd.unpersist()
```

### Checkpoint

```scala
// 设置 Checkpoint 目录
sc.setCheckpointDir("hdfs://namenode:8021/checkpoint")

// Checkpoint RDD
rdd.checkpoint()

// 触发计算
rdd.count()
```

---

## 完整应用示例

### 日志分析

```scala
import org.apache.spark.{SparkConf, SparkContext}

object LogAnalysis {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Log Analysis")
    val sc = new SparkContext(conf)

    // 读取日志文件
    val logs = sc.textFile("hdfs://path/to/logs/*.log")

    // 解析日志
    val parsedLogs = logs.map { line =>
      val parts = line.split(" ")
      (parts(0), parts(1), parts(2))  // (timestamp, level, message)
    }

    // 统计错误数量
    val errorCount = parsedLogs
      .filter(_._2 == "ERROR")
      .count()

    // 按小时统计
    val hourlyStats = parsedLogs
      .map { case (timestamp, level, message) =>
        val hour = timestamp.substring(0, 13)
        (hour, 1)
      }
      .reduceByKey(_ + _)
      .sortByKey()

    // 保存结果
    hourlyStats.saveAsTextFile("hdfs://path/to/output")

    println(s"Total errors: $errorCount")
    sc.stop()
  }
}
```

### 实时推荐

```scala
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._

object RealtimeRecommendation {
  def main(args: Array[String]) {
    val ssc = new StreamingContext(sc, Seconds(10))
    ssc.checkpoint("hdfs://path/to/checkpoint")

    // 从 Kafka 读取用户行为
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics
    )

    // 解析用户行为
    val userActions = stream.map { case (key, value) =>
      val parts = value.split(",")
      (parts(0), parts(1), parts(2))  // (userId, itemId, action)
    }

    // 统计用户行为
    val userStats = userActions
      .map { case (userId, itemId, action) => ((userId, itemId), 1) }
      .reduceByKeyAndWindow(_ + _, Seconds(300), Seconds(10))

    // 生成推荐
    userStats.foreachRDD { rdd =>
      rdd.foreach { case ((userId, itemId), count) =>
        if (count > 5) {
          // 推送推荐
          sendRecommendation(userId, itemId)
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
```

---

## 参考资料

- [Spark Programming Guide](http://spark.apache.org/docs/1.6.3/programming-guide.html)
- [Spark SQL Guide](http://spark.apache.org/docs/1.6.3/sql-programming-guide.html)
- [Spark Streaming Guide](http://spark.apache.org/docs/1.6.3/streaming-programming-guide.html)
- [Spark Examples](https://github.com/apache/spark/tree/v1.6.3/examples)

---

**下一步**：查看 [FAQ 和最佳实践](FAQ.md) 了解常见问题和解决方案。
