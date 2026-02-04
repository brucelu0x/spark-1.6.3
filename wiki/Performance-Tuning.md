# Spark 性能调优

本文档提供 Apache Spark 1.6.3 的性能调优指南和最佳实践。

## 目录

- [性能调优概述](#性能调优概述)
- [数据序列化](#数据序列化)
- [内存调优](#内存调优)
- [数据本地性](#数据本地性)
- [并行度调优](#并行度调优)
- [Shuffle 优化](#shuffle-优化)
- [广播变量](#广播变量)
- [缓存策略](#缓存策略)
- [数据倾斜处理](#数据倾斜处理)
- [SQL 优化](#sql-优化)
- [Streaming 优化](#streaming-优化)

---

## 性能调优概述

### 性能瓶颈识别

使用 Spark UI 识别性能瓶颈：

1. **Jobs 页面**：查看 Job 执行时间
2. **Stages 页面**：查看 Stage 执行时间和 Task 分布
3. **Storage 页面**：查看 RDD 缓存情况
4. **Executors 页面**：查看 Executor 资源使用
5. **SQL 页面**：查看 SQL 查询计划

### 常见性能问题

- **数据倾斜**：某些 Task 执行时间远超其他 Task
- **Shuffle 过多**：大量数据在网络间传输
- **内存不足**：频繁 GC 或 OOM
- **并行度不足**：CPU 利用率低
- **数据本地性差**：大量数据跨节点传输

---

## 数据序列化

### 选择序列化器

Spark 支持两种序列化器：

#### 1. Java 序列化（默认）

```scala
// 默认使用 Java 序列化
val conf = new SparkConf()
```

**优点**：
- 兼容性好，支持所有 Java 类
- 无需注册类

**缺点**：
- 速度慢
- 序列化后体积大

#### 2. Kryo 序列化（推荐）

```scala
val conf = new SparkConf()
  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .set("spark.kryo.registrationRequired", "false")
```

**优点**：
- 速度快（比 Java 序列化快 10 倍）
- 序列化后体积小

**缺点**：
- 需要注册自定义类
- 不支持所有类型

### 注册自定义类

```scala
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[MyClass1])
    kryo.register(classOf[MyClass2])
  }
}

val conf = new SparkConf()
  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .set("spark.kryo.registrator", "com.mycompany.MyRegistrator")
```

### 性能对比

| 序列化器 | 序列化时间 | 反序列化时间 | 大小 |
|----------|-----------|-------------|------|
| Java | 100ms | 100ms | 100MB |
| Kryo | 10ms | 10ms | 30MB |

---

## 内存调优

### 内存模型

Spark 1.6.3 使用统一内存管理（Unified Memory Management）：

```
Executor Memory (4GB)
├── Reserved (300MB)
└── Usable (3.7GB)
    ├── Execution (60%) - 2.22GB
    │   ├── Shuffle
    │   ├── Join
    │   └── Sort
    └── Storage (40%) - 1.48GB
        ├── Cache
        └── Broadcast
```

### 内存配置建议

```properties
# Executor 内存
spark.executor.memory=4g

# 内存分配比例
spark.memory.fraction=0.75
spark.memory.storageFraction=0.5

# 堆外内存（可选）
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=2g
```

### 避免 OOM

**1. 增加 Executor 内存**

```bash
./bin/spark-submit \
  --executor-memory 8G \
  myapp.jar
```

**2. 增加分区数**

```scala
// 增加并行度，减少每个 Task 处理的数据量
val rdd = sc.textFile("data.txt", 200)
```

**3. 使用 mapPartitions 代替 map**

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

### GC 调优

**使用 G1GC（推荐）**

```properties
spark.executor.extraJavaOptions=-XX:+UseG1GC \
  -XX:MaxGCPauseMillis=200 \
  -XX:InitiatingHeapOccupancyPercent=45 \
  -XX:+PrintGCDetails \
  -XX:+PrintGCTimeStamps
```

**监控 GC**

```bash
# 查看 GC 日志
tail -f /var/log/spark/gc.log
```

**GC 调优指标**：

- GC 时间占比 < 10%
- Full GC 频率低
- GC 暂停时间 < 1s

---

## 数据本地性

### 本地性级别

从高到低：

1. **PROCESS_LOCAL**：数据在同一个 JVM 进程（最快）
2. **NODE_LOCAL**：数据在同一个节点
3. **RACK_LOCAL**：数据在同一个机架
4. **ANY**：数据在任意位置（最慢）

### 提高数据本地性

**1. 增加本地性等待时间**

```properties
spark.locality.wait=3s
spark.locality.wait.node=3s
spark.locality.wait.process=3s
```

**2. 增加 Executor 数量**

```properties
# 更多 Executor 意味着更多机会实现本地性
spark.executor.instances=20
```

**3. 合理设置分区数**

```scala
// 分区数 = Executor 数量 * 每个 Executor 的核心数 * 2-3
val partitions = 20 * 4 * 2 // 160
val rdd = sc.textFile("data.txt", partitions)
```

---

## 并行度调优

### 设置并行度

**1. 默认并行度**

```properties
# 影响 parallelize, range, makeRDD
spark.default.parallelism=200
```

**2. SQL Shuffle 分区数**

```properties
# 影响 SQL 的 Shuffle 操作
spark.sql.shuffle.partitions=200
```

### 并行度计算公式

```
推荐并行度 = Executor 数量 × 每个 Executor 的核心数 × 2-3

示例：
- 10 个 Executor
- 每个 Executor 4 核心
- 推荐并行度 = 10 × 4 × 2 = 80-120
```

### 重新分区

**增加分区数（repartition）**

```scala
// 数据量大，分区数少
val rdd = sc.textFile("data.txt", 10)

// 增加分区数，提高并行度
val repartitioned = rdd.repartition(100)
```

**减少分区数（coalesce）**

```scala
// 过滤后数据量减少
val filtered = rdd.filter(_.contains("error"))

// 减少分区数，避免小文件
val coalesced = filtered.coalesce(10)
```

**区别**：

- `repartition`：会触发 Shuffle，可以增加或减少分区
- `coalesce`：不触发 Shuffle（默认），只能减少分区

---

## Shuffle 优化

### Shuffle 原理

```
Map 端：
[Task1] → [Partition 0, 1, 2]
[Task2] → [Partition 0, 1, 2]
[Task3] → [Partition 0, 1, 2]
         ↓ Shuffle ↓
Reduce 端：
[Task1] ← [Partition 0]
[Task2] ← [Partition 1]
[Task3] ← [Partition 2]
```

### 减少 Shuffle

**1. 使用 reduceByKey 代替 groupByKey**

```scala
// 不好：先 group 再 reduce，Shuffle 数据量大
rdd.groupByKey().mapValues(_.sum)

// 好：先 reduce 再 Shuffle，Shuffle 数据量小
rdd.reduceByKey(_ + _)
```

**2. 使用 aggregateByKey**

```scala
// 更灵活的聚合操作
rdd.aggregateByKey(0)(
  (acc, value) => acc + value,  // 分区内聚合
  (acc1, acc2) => acc1 + acc2   // 分区间聚合
)
```

**3. 使用 mapPartitions**

```scala
// 减少函数调用次数
rdd.mapPartitions { partition =>
  partition.map(process)
}
```

### Shuffle 配置优化

```properties
# 增加 Shuffle 缓冲区
spark.shuffle.file.buffer=64k

# 增加 Reduce 端拉取数据量
spark.reducer.maxSizeInFlight=96m

# 启用 Shuffle 压缩
spark.shuffle.compress=true
spark.shuffle.spill.compress=true

# 选择压缩算法（lz4 最快）
spark.io.compression.codec=lz4
```

---

## 广播变量

### 使用场景

当需要在所有 Executor 上共享一个只读变量时，使用广播变量：

```scala
// 不好：每个 Task 都会传输 lookup 表
val lookup = Map("a" -> 1, "b" -> 2, "c" -> 3)
rdd.map(x => lookup.get(x))

// 好：只传输一次到每个 Executor
val broadcastLookup = sc.broadcast(lookup)
rdd.map(x => broadcastLookup.value.get(x))
```

### 广播 Join

```scala
// 小表 broadcast join 大表
val smallDF = spark.read.json("small.json")
val largeDF = spark.read.json("large.json")

// 自动广播（小于 10MB）
val joined = largeDF.join(smallDF, "key")

// 手动广播
import org.apache.spark.sql.functions.broadcast
val joined = largeDF.join(broadcast(smallDF), "key")
```

### 广播变量配置

```properties
# 自动广播阈值
spark.sql.autoBroadcastJoinThreshold=10485760  # 10MB

# 广播超时时间
spark.sql.broadcastTimeout=300

# 广播块大小
spark.broadcast.blockSize=4m
```

---

## 缓存策略

### 存储级别

```scala
import org.apache.spark.storage.StorageLevel

// 仅内存
rdd.persist(StorageLevel.MEMORY_ONLY)

// 内存 + 磁盘
rdd.persist(StorageLevel.MEMORY_AND_DISK)

// 内存 + 序列化
rdd.persist(StorageLevel.MEMORY_ONLY_SER)

// 内存 + 磁盘 + 序列化
rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)

// 仅磁盘
rdd.persist(StorageLevel.DISK_ONLY)

// 带副本（容错）
rdd.persist(StorageLevel.MEMORY_ONLY_2)
```

### 选择存储级别

| 场景 | 推荐存储级别 | 原因 |
|------|-------------|------|
| 内存充足 | MEMORY_ONLY | 最快 |
| 内存不足 | MEMORY_AND_DISK_SER | 节省内存 |
| 数据量大 | MEMORY_AND_DISK | 避免重算 |
| 容错要求高 | MEMORY_ONLY_2 | 有副本 |

### 缓存最佳实践

**1. 缓存频繁使用的 RDD**

```scala
val rdd = sc.textFile("data.txt")
  .map(process)
  .filter(_.nonEmpty)
  .cache()  // 缓存

// 多次使用
val count1 = rdd.count()
val count2 = rdd.filter(_.contains("error")).count()
```

**2. 及时释放缓存**

```scala
// 使用完后释放
rdd.unpersist()
```

**3. 监控缓存使用**

在 Spark UI 的 Storage 页面查看：
- 缓存的 RDD
- 内存使用情况
- 缓存命中率

---

## 数据倾斜处理

### 识别数据倾斜

**症状**：

- 某些 Task 执行时间远超其他 Task
- Shuffle Read 数据量分布不均
- 某些 Executor 内存使用率高

**查看方式**：

在 Spark UI 的 Stages 页面查看 Task 执行时间分布。

### 解决方案

#### 1. 增加并行度

```scala
// 增加分区数，减少每个 Task 处理的数据量
val rdd = data.repartition(500)
```

#### 2. 使用随机前缀

```scala
// 原始数据倾斜
val skewed = rdd.groupByKey()

// 添加随机前缀
val prefixed = rdd.map { case (k, v) =>
  val prefix = Random.nextInt(10)
  ((prefix, k), v)
}

// 两阶段聚合
val partial = prefixed.reduceByKey(_ + _)
val result = partial.map { case ((prefix, k), v) =>
  (k, v)
}.reduceByKey(_ + _)
```

#### 3. 过滤异常数据

```scala
// 过滤掉导致倾斜的 key
val filtered = rdd.filter { case (k, v) =>
  k != "skewed_key"
}
```

#### 4. 分离处理

```scala
// 分离倾斜 key 单独处理
val skewedKeys = Set("key1", "key2")

val normal = rdd.filter { case (k, v) => !skewedKeys.contains(k) }
val skewed = rdd.filter { case (k, v) => skewedKeys.contains(k) }

// 正常数据正常处理
val normalResult = normal.reduceByKey(_ + _)

// 倾斜数据特殊处理（如单机处理）
val skewedResult = skewed.groupByKey().mapValues(_.sum)

// 合并结果
val result = normalResult.union(skewedResult)
```

---

## SQL 优化

### 查询优化

**1. 谓词下推（Predicate Pushdown）**

```scala
// 好：过滤条件下推到数据源
df.filter($"age" > 21).select("name", "age")

// 不好：先读取所有数据再过滤
df.select("name", "age").filter($"age" > 21)
```

**2. 列裁剪（Column Pruning）**

```scala
// 好：只读取需要的列
df.select("name", "age")

// 不好：读取所有列
df.select("*").drop("unused_column")
```

**3. 分区裁剪（Partition Pruning）**

```scala
// 数据按日期分区
val df = spark.read.parquet("data/")

// 好：只读取特定分区
df.filter($"date" === "2023-01-01")

// 不好：读取所有分区再过滤
df.filter($"event_type" === "click")
```

### Join 优化

**1. Broadcast Join**

```scala
// 小表 join 大表
val result = largeDF.join(broadcast(smallDF), "key")
```

**2. Sort Merge Join**

```scala
// 大表 join 大表，数据已排序
val result = df1.join(df2, "key")
```

**3. 避免笛卡尔积**

```scala
// 不好：笛卡尔积
df1.join(df2)

// 好：指定 join 条件
df1.join(df2, "key")
```

### 数据格式选择

| 格式 | 读取速度 | 写入速度 | 压缩率 | 列式存储 |
|------|---------|---------|--------|---------|
| Text | 慢 | 快 | 低 | 否 |
| JSON | 慢 | 中 | 低 | 否 |
| Parquet | 快 | 中 | 高 | 是 |
| ORC | 快 | 中 | 高 | 是 |

**推荐**：使用 Parquet 格式

```scala
// 写入 Parquet
df.write.parquet("output.parquet")

// 读取 Parquet
val df = spark.read.parquet("output.parquet")
```

---

## Streaming 优化

### 批次间隔

```scala
// 批次间隔不宜过小
val ssc = new StreamingContext(sc, Seconds(5))
```

**建议**：

- 批次间隔 >= 500ms
- 处理时间 < 批次间隔

### 反压机制

```properties
# 启用反压
spark.streaming.backpressure.enabled=true

# 初始速率
spark.streaming.backpressure.initialRate=100000
```

### 接收器优化

```properties
# 增加接收器并行度
spark.streaming.receiver.maxRate=10000

# 启用 WAL
spark.streaming.receiver.writeAheadLog.enable=true
```

### Kafka Direct 方式

```scala
// 使用 Direct 方式（推荐）
val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
  ssc, kafkaParams, topics
)
```

**优点**：

- 无需 Receiver
- 更好的容错性
- 精确一次语义

---

## 性能监控

### 关键指标

1. **Job 执行时间**
2. **Stage 执行时间**
3. **Task 执行时间分布**
4. **Shuffle Read/Write 大小**
5. **GC 时间占比**
6. **内存使用率**
7. **CPU 使用率**

### 监控工具

- **Spark UI**：实时监控
- **Spark History Server**：历史记录
- **Ganglia/Graphite**：集群监控
- **JMX**：JVM 监控

---

## 性能调优检查清单

- [ ] 使用 Kryo 序列化
- [ ] 合理设置 Executor 内存和核心数
- [ ] 设置合适的并行度
- [ ] 减少 Shuffle 操作
- [ ] 使用广播变量
- [ ] 缓存频繁使用的 RDD
- [ ] 处理数据倾斜
- [ ] 使用列式存储格式（Parquet）
- [ ] 启用 SQL 自动优化
- [ ] 监控 GC 时间
- [ ] 提高数据本地性
- [ ] 避免使用 groupByKey

---

## 参考资料

- [Spark Tuning 官方文档](http://spark.apache.org/docs/1.6.3/tuning.html)
- [配置参考](Configuration.md)
- [架构与设计](Architecture.md)

---

**下一步**：查看 [运维手册](Operations.md) 了解 Spark 集群的部署和运维。
