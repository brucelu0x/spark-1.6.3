# FAQ 和最佳实践

本文档提供 Apache Spark 1.6.3 的常见问题解答和最佳实践。

## 目录

- [常见问题](#常见问题)
- [最佳实践](#最佳实践)
- [性能优化](#性能优化)
- [故障排查](#故障排查)
- [开发建议](#开发建议)

---

## 常见问题

### Q1: 如何选择 Spark 版本？

**A**: Spark 1.6.3 是 1.6 系列的稳定版本，适合：

- 需要稳定性的生产环境
- 使用 Scala 2.10 的项目
- 需要与旧版 Hadoop 集成

**注意**：Spark 1.6.x 已经较老，建议评估升级到 2.x 或 3.x 版本。

### Q2: 如何估算资源需求？

**A**: 资源估算公式：

```
Executor 内存 = (数据量 / 并行度) × 3-4

示例：
- 数据量：100GB
- 并行度：100
- Executor 内存 = (100GB / 100) × 3 = 3GB
```

**建议配置**：

| 数据量 | Executor 内存 | Executor 核心 | Executor 数量 |
|--------|--------------|--------------|--------------|
| < 10GB | 2G | 2 | 5 |
| 10-100GB | 4G | 4 | 10 |
| > 100GB | 8G | 5 | 20+ |

### Q3: 如何处理小文件问题？

**A**: 小文件会导致大量 Task，影响性能。

**解决方案**：

```scala
// 1. 合并小文件
rdd.coalesce(10).saveAsTextFile("output")

// 2. 使用 repartition
rdd.repartition(10).saveAsTextFile("output")

// 3. 使用 Hadoop 的 CombineFileInputFormat
sc.hadoopConfiguration.set(
  "mapreduce.input.fileinputformat.split.maxsize",
  "134217728"  // 128MB
)
```

### Q4: groupByKey vs reduceByKey 有什么区别？

**A**:

**groupByKey**：
- 先 Shuffle 所有数据，再聚合
- Shuffle 数据量大
- 内存压力大

**reduceByKey**：
- 先在 Map 端预聚合，再 Shuffle
- Shuffle 数据量小
- 性能更好

**示例**：

```scala
// 不好：groupByKey
rdd.groupByKey().mapValues(_.sum)

// 好：reduceByKey
rdd.reduceByKey(_ + _)
```

### Q5: 如何优化 Join 操作？

**A**:

**1. Broadcast Join（小表 join 大表）**

```scala
import org.apache.spark.sql.functions.broadcast
largeDF.join(broadcast(smallDF), "key")
```

**2. 预分区（大表 join 大表）**

```scala
// 使用相同的分区器
val partitioner = new HashPartitioner(100)
val rdd1Partitioned = rdd1.partitionBy(partitioner)
val rdd2Partitioned = rdd2.partitionBy(partitioner)

// Join 不会触发 Shuffle
val joined = rdd1Partitioned.join(rdd2Partitioned)
```

**3. 避免笛卡尔积**

```scala
// 不好：笛卡尔积
df1.join(df2)

// 好：指定 join 条件
df1.join(df2, "key")
```

### Q6: 如何选择存储级别？

**A**:

| 场景 | 存储级别 | 原因 |
|------|---------|------|
| 内存充足，数据量小 | MEMORY_ONLY | 最快 |
| 内存不足，数据量大 | MEMORY_AND_DISK_SER | 节省内存，避免重算 |
| 数据重算成本低 | 不缓存 | 节省内存 |
| 容错要求高 | MEMORY_ONLY_2 | 有副本 |

### Q7: 如何处理数据倾斜？

**A**: 参考 [性能调优 - 数据倾斜处理](Performance-Tuning.md#数据倾斜处理)

**快速方案**：

```scala
// 1. 增加并行度
rdd.repartition(500)

// 2. 添加随机前缀
rdd.map { case (k, v) =>
  val prefix = Random.nextInt(10)
  ((prefix, k), v)
}.reduceByKey(_ + _)
```

### Q8: 如何选择数据格式？

**A**:

| 格式 | 优点 | 缺点 | 适用场景 |
|------|------|------|---------|
| Text | 简单，兼容性好 | 慢，无 Schema | 日志文件 |
| JSON | 可读性好，有 Schema | 慢，体积大 | 配置文件 |
| Parquet | 快，压缩率高，列式存储 | 不可读 | 数据仓库（推荐） |
| ORC | 快，压缩率高 | Hive 专用 | Hive 表 |

**推荐**：使用 Parquet 格式

### Q9: 如何调试 Spark 应用？

**A**:

**1. 本地调试**

```scala
val conf = new SparkConf()
  .setMaster("local[*]")
  .setAppName("Debug")

val sc = new SparkContext(conf)
```

**2. 查看 Spark UI**

- Jobs 页面：查看 Job 执行时间
- Stages 页面：查看 Task 分布
- Storage 页面：查看缓存情况

**3. 添加日志**

```scala
rdd.foreach(x => println(s"Processing: $x"))
```

**4. 使用 take 采样**

```scala
// 只处理前 100 条数据
rdd.take(100).foreach(println)
```

### Q10: 如何提交 Spark 应用？

**A**:

```bash
./bin/spark-submit \
  --class com.example.MyApp \
  --master spark://master:7077 \
  --deploy-mode cluster \
  --executor-memory 4G \
  --executor-cores 4 \
  --num-executors 10 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.shuffle.partitions=200 \
  myapp.jar \
  arg1 arg2
```

---

## 最佳实践

### 1. 代码编写

#### 使用 Kryo 序列化

```scala
val conf = new SparkConf()
  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .set("spark.kryo.registrationRequired", "false")
```

#### 避免在 Driver 端收集大量数据

```scala
// 不好：collect 大量数据到 Driver
val data = rdd.collect()  // OOM!

// 好：在 Executor 端处理
rdd.foreach(process)

// 或者采样
val sample = rdd.take(100)
```

#### 使用 mapPartitions 代替 map

```scala
// 不好：每条记录创建一个连接
rdd.map { record =>
  val conn = createConnection()
  process(conn, record)
}

// 好：每个分区创建一个连接
rdd.mapPartitions { partition =>
  val conn = createConnection()
  partition.map(record => process(conn, record))
}
```

#### 及时释放资源

```scala
// 释放缓存
rdd.unpersist()

// 释放广播变量
broadcastVar.unpersist()

// 停止 SparkContext
sc.stop()
```

### 2. 资源配置

#### 合理设置 Executor 资源

```properties
# 推荐配置
spark.executor.memory=4g
spark.executor.cores=4
spark.executor.instances=10

# 避免单个 Executor 过大
# 不好：spark.executor.memory=32g
# 好：增加 Executor 数量，减小单个 Executor 内存
```

#### 设置合适的并行度

```properties
# 并行度 = Executor 数量 × 每个 Executor 的核心数 × 2-3
spark.default.parallelism=200
spark.sql.shuffle.partitions=200
```

#### 启用动态资源分配

```properties
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=2
spark.dynamicAllocation.maxExecutors=20
spark.shuffle.service.enabled=true
```

### 3. 性能优化

#### 减少 Shuffle

```scala
// 使用 reduceByKey 代替 groupByKey
rdd.reduceByKey(_ + _)

// 使用 aggregateByKey
rdd.aggregateByKey(0)(_ + _, _ + _)

// 使用 combineByKey
rdd.combineByKey(
  (v) => v,
  (acc: Int, v) => acc + v,
  (acc1: Int, acc2: Int) => acc1 + acc2
)
```

#### 使用广播变量

```scala
// 广播小表
val broadcastVar = sc.broadcast(smallTable)
rdd.map(x => broadcastVar.value.get(x))
```

#### 缓存频繁使用的 RDD

```scala
val rdd = sc.textFile("data.txt")
  .map(process)
  .cache()

// 多次使用
val count1 = rdd.count()
val count2 = rdd.filter(_.contains("error")).count()
```

### 4. 数据管理

#### 使用列式存储格式

```scala
// 写入 Parquet
df.write.parquet("output.parquet")

// 读取 Parquet
val df = spark.read.parquet("output.parquet")
```

#### 分区数据

```scala
// 按日期分区
df.write
  .partitionBy("date")
  .parquet("output.parquet")

// 读取特定分区
val df = spark.read.parquet("output.parquet")
  .filter($"date" === "2023-01-01")
```

#### 压缩数据

```properties
# 启用压缩
spark.sql.parquet.compression.codec=snappy
spark.io.compression.codec=lz4
```

### 5. 监控和调试

#### 启用 Event Log

```properties
spark.eventLog.enabled=true
spark.eventLog.dir=hdfs://namenode:8021/spark-logs
```

#### 监控 GC

```properties
spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps
```

#### 使用 Spark UI

定期查看：
- Job 执行时间
- Stage 执行时间
- Task 分布
- Shuffle 数据量
- GC 时间

---

## 性能优化

### 优化检查清单

- [ ] 使用 Kryo 序列化
- [ ] 设置合适的并行度
- [ ] 减少 Shuffle 操作
- [ ] 使用广播变量
- [ ] 缓存频繁使用的 RDD
- [ ] 使用列式存储格式
- [ ] 处理数据倾斜
- [ ] 避免使用 groupByKey
- [ ] 使用 mapPartitions
- [ ] 及时释放资源
- [ ] 监控 GC 时间
- [ ] 提高数据本地性

### 性能调优流程

1. **识别瓶颈**：使用 Spark UI 查看性能指标
2. **分析原因**：确定是 CPU、内存、网络还是磁盘瓶颈
3. **应用优化**：根据瓶颈类型应用相应优化
4. **验证效果**：重新运行并对比性能指标
5. **迭代优化**：重复上述步骤直到满意

---

## 故障排查

### 常见错误

#### 1. OutOfMemoryError

**原因**：
- Executor 内存不足
- Driver 内存不足
- 数据倾斜

**解决方案**：
```bash
# 增加内存
--executor-memory 8G
--driver-memory 4G

# 增加分区数
--conf spark.sql.shuffle.partitions=400
```

#### 2. Shuffle Fetch Failed

**原因**：
- 网络超时
- Executor 失败
- 磁盘空间不足

**解决方案**：
```properties
spark.shuffle.io.maxRetries=5
spark.shuffle.io.retryWait=10s
spark.network.timeout=300s
```

#### 3. Task 超时

**原因**：
- 数据倾斜
- 网络问题
- 资源不足

**解决方案**：
```properties
spark.network.timeout=600s
spark.executor.heartbeatInterval=20s
```

---

## 开发建议

### 1. 项目结构

```
myapp/
├── src/
│   ├── main/
│   │   ├── scala/
│   │   │   └── com/example/
│   │   │       ├── Main.scala
│   │   │       ├── jobs/
│   │   │       ├── utils/
│   │   │       └── config/
│   │   └── resources/
│   │       └── application.conf
│   └── test/
│       └── scala/
│           └── com/example/
│               └── MainTest.scala
├── build.sbt
└── README.md
```

### 2. 配置管理

使用配置文件管理参数：

```scala
import com.typesafe.config.ConfigFactory

object Config {
  private val config = ConfigFactory.load()

  val sparkMaster = config.getString("spark.master")
  val executorMemory = config.getString("spark.executor.memory")
  val inputPath = config.getString("app.input.path")
  val outputPath = config.getString("app.output.path")
}
```

### 3. 单元测试

```scala
import org.scalatest.FunSuite
import org.apache.spark.{SparkConf, SparkContext}

class MyAppTest extends FunSuite {
  test("word count") {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Test")

    val sc = new SparkContext(conf)

    val input = sc.parallelize(Seq("hello world", "hello spark"))
    val result = input
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .collect()
      .toMap

    assert(result("hello") == 2)
    assert(result("world") == 1)
    assert(result("spark") == 1)

    sc.stop()
  }
}
```

### 4. 日志管理

```scala
import org.apache.log4j.{Level, Logger}

object MyApp {
  def main(args: Array[String]) {
    // 设置日志级别
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val logger = Logger.getLogger(getClass.getName)
    logger.info("Application started")

    // 应用逻辑
    ...

    logger.info("Application finished")
  }
}
```

---

## 参考资料

- [Spark FAQ](http://spark.apache.org/faq.html)
- [Spark Best Practices](https://spark.apache.org/docs/1.6.3/tuning.html)
- [性能调优](Performance-Tuning.md)
- [运维手册](Operations.md)

---

**下一步**：查看 [开发者资源](Developer-Resources.md) 深入了解 Spark 源码。
