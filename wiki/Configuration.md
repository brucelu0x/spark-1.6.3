# Spark 配置参考

本文档提供 Apache Spark 1.6.3 的完整配置参考和最佳实践。

## 目录

- [配置方式](#配置方式)
- [核心配置](#核心配置)
- [执行配置](#执行配置)
- [内存配置](#内存配置)
- [Shuffle 配置](#shuffle配置)
- [网络配置](#网络配置)
- [调度配置](#调度配置)
- [安全配置](#安全配置)
- [监控配置](#监控配置)
- [Spark SQL 配置](#spark-sql-配置)
- [Spark Streaming 配置](#spark-streaming-配置)

---

## 配置方式

Spark 提供三种配置方式（优先级从高到低）：

### 1. 代码中设置

```scala
val conf = new SparkConf()
  .setAppName("MyApp")
  .setMaster("local[*]")
  .set("spark.executor.memory", "4g")
  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

val sc = new SparkContext(conf)
```

### 2. spark-submit 命令行参数

```bash
./bin/spark-submit \
  --master spark://master:7077 \
  --executor-memory 4G \
  --total-executor-cores 100 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  myapp.jar
```

### 3. 配置文件

**文件位置**：`conf/spark-defaults.conf`

```properties
spark.master                     spark://master:7077
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs://namenode:8021/directory
spark.serializer                 org.apache.spark.serializer.KryoSerializer
spark.driver.memory              5g
```

---

## 核心配置

### 应用基本信息

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.app.name` | (none) | 应用名称，显示在 UI 和日志中 |
| `spark.master` | (none) | 集群管理器地址：`local`, `local[N]`, `spark://HOST:PORT`, `yarn`, `mesos://HOST:PORT` |
| `spark.submit.deployMode` | client | 部署模式：`client` 或 `cluster` |
| `spark.driver.cores` | 1 | Driver 使用的 CPU 核心数（仅 cluster 模式） |
| `spark.driver.memory` | 1g | Driver 内存大小 |
| `spark.driver.maxResultSize` | 1g | 每个 Action 操作返回结果的最大大小 |

**示例**：

```properties
spark.app.name=MySparkApp
spark.master=spark://master:7077
spark.driver.memory=2g
spark.driver.cores=2
```

### Executor 配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.executor.memory` | 1g | 每个 Executor 的内存大小 |
| `spark.executor.cores` | 1 | 每个 Executor 使用的 CPU 核心数 |
| `spark.executor.instances` | 2 | Executor 数量（静态分配） |
| `spark.executor.heartbeatInterval` | 10s | Executor 向 Driver 发送心跳的间隔 |
| `spark.executor.extraJavaOptions` | (none) | Executor 的 JVM 参数 |
| `spark.executor.extraClassPath` | (none) | Executor 的额外 classpath |
| `spark.executor.extraLibraryPath` | (none) | Executor 的额外库路径 |

**示例**：

```properties
spark.executor.memory=4g
spark.executor.cores=4
spark.executor.instances=10
spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps
```

---

## 执行配置

### 并行度

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.default.parallelism` | 取决于集群 | 默认并行度，影响 `parallelize`, `range`, `makeRDD` 等操作 |
| `spark.sql.shuffle.partitions` | 200 | SQL Shuffle 操作的分区数 |
| `spark.files.maxPartitionBytes` | 128MB | 读取文件时单个分区的最大字节数 |

**建议值**：

- `spark.default.parallelism`：CPU 核心总数的 2-3 倍
- `spark.sql.shuffle.partitions`：根据数据量调整，小数据集可以设置为 50-100

```properties
spark.default.parallelism=200
spark.sql.shuffle.partitions=200
```

### 任务调度

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.scheduler.mode` | FIFO | 调度模式：`FIFO` 或 `FAIR` |
| `spark.scheduler.allocation.file` | (none) | Fair Scheduler 配置文件路径 |
| `spark.locality.wait` | 3s | 本地性等待时间 |
| `spark.locality.wait.node` | spark.locality.wait | 节点本地性等待时间 |
| `spark.locality.wait.process` | spark.locality.wait | 进程本地性等待时间 |
| `spark.locality.wait.rack` | spark.locality.wait | 机架本地性等待时间 |

**本地性级别**（从高到低）：

1. `PROCESS_LOCAL`：数据在同一个 JVM 进程
2. `NODE_LOCAL`：数据在同一个节点
3. `RACK_LOCAL`：数据在同一个机架
4. `ANY`：数据在任意位置

```properties
spark.scheduler.mode=FAIR
spark.locality.wait=3s
```

---

## 内存配置

### 内存模型

Spark 1.6.3 的内存分为三部分：

```
┌─────────────────────────────────────┐
│  Executor Memory (spark.executor.memory)
├─────────────────────────────────────┤
│  Execution Memory (执行内存)         │ 60%
│  - Shuffle, Join, Sort, Aggregation │
├─────────────────────────────────────┤
│  Storage Memory (存储内存)           │ 40%
│  - Cache, Broadcast                 │
├─────────────────────────────────────┤
│  User Memory (用户内存)              │
│  - 用户数据结构                      │
├─────────────────────────────────────┤
│  Reserved Memory (保留内存)          │ 300MB
└─────────────────────────────────────┘
```

### 内存配置项

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.memory.fraction` | 0.75 | 执行和存储内存占总内存的比例 |
| `spark.memory.storageFraction` | 0.5 | 存储内存占 (执行+存储) 内存的比例 |
| `spark.memory.offHeap.enabled` | false | 是否启用堆外内存 |
| `spark.memory.offHeap.size` | 0 | 堆外内存大小 |

**计算公式**：

```
可用内存 = (Executor Memory - 300MB) * spark.memory.fraction
执行内存 = 可用内存 * (1 - spark.memory.storageFraction)
存储内存 = 可用内存 * spark.memory.storageFraction
```

**示例**：

```properties
# Executor 内存 4GB
spark.executor.memory=4g

# 可用内存 = (4096 - 300) * 0.75 = 2847 MB
spark.memory.fraction=0.75

# 存储内存 = 2847 * 0.5 = 1423.5 MB
# 执行内存 = 2847 * 0.5 = 1423.5 MB
spark.memory.storageFraction=0.5
```

### 垃圾回收配置

```properties
# Executor GC 配置
spark.executor.extraJavaOptions=-XX:+UseG1GC \
  -XX:+PrintGCDetails \
  -XX:+PrintGCTimeStamps \
  -XX:+PrintGCDateStamps \
  -XX:+PrintHeapAtGC \
  -Xloggc:/var/log/spark/gc.log

# Driver GC 配置
spark.driver.extraJavaOptions=-XX:+UseG1GC \
  -XX:+PrintGCDetails \
  -XX:+PrintGCTimeStamps
```

---

## Shuffle 配置

### Shuffle 行为

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.shuffle.manager` | sort | Shuffle 管理器：`sort` 或 `tungsten-sort` |
| `spark.shuffle.sort.bypassMergeThreshold` | 200 | 分区数小于此值时使用 bypass 机制 |
| `spark.shuffle.compress` | true | 是否压缩 Shuffle 输出 |
| `spark.shuffle.spill.compress` | true | 是否压缩溢写文件 |
| `spark.shuffle.file.buffer` | 32k | Shuffle 文件缓冲区大小 |
| `spark.reducer.maxSizeInFlight` | 48m | Reduce 端同时拉取的最大数据量 |
| `spark.shuffle.io.maxRetries` | 3 | Shuffle 读取失败重试次数 |
| `spark.shuffle.io.retryWait` | 5s | Shuffle 读取重试等待时间 |

**优化建议**：

```properties
# 大数据量场景
spark.shuffle.file.buffer=64k
spark.reducer.maxSizeInFlight=96m

# 网络不稳定场景
spark.shuffle.io.maxRetries=5
spark.shuffle.io.retryWait=10s
```

### Shuffle 内存

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.shuffle.memoryFraction` | 0.2 | Shuffle 聚合使用的内存比例（已废弃） |
| `spark.shuffle.spill` | true | 内存不足时是否溢写到磁盘 |

---

## 网络配置

### 网络传输

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.network.timeout` | 120s | 网络超时时间 |
| `spark.rpc.askTimeout` | spark.network.timeout | RPC 请求超时 |
| `spark.rpc.lookupTimeout` | 120s | RPC 查找超时 |
| `spark.core.connection.ack.wait.timeout` | spark.network.timeout | 连接确认超时 |
| `spark.akka.frameSize` | 128 | Akka 消息最大大小（MB） |
| `spark.akka.threads` | 4 | Akka 线程数 |
| `spark.akka.timeout` | 100s | Akka 超时时间 |

**大集群配置**：

```properties
spark.network.timeout=300s
spark.rpc.askTimeout=300s
spark.akka.frameSize=256
```

### 块传输

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.blockManager.port` | random | BlockManager 端口 |
| `spark.broadcast.blockSize` | 4m | 广播变量的块大小 |
| `spark.io.compression.codec` | lz4 | 压缩编解码器：`lz4`, `lzf`, `snappy` |
| `spark.io.compression.lz4.blockSize` | 32k | LZ4 压缩块大小 |
| `spark.io.compression.snappy.blockSize` | 32k | Snappy 压缩块大小 |

---

## 调度配置

### Fair Scheduler

**配置文件**：`conf/fairscheduler.xml`

```xml
<?xml version="1.0"?>
<allocations>
  <pool name="production">
    <schedulingMode>FAIR</schedulingMode>
    <weight>2</weight>
    <minShare>2</minShare>
  </pool>
  <pool name="test">
    <schedulingMode>FIFO</schedulingMode>
    <weight>1</weight>
    <minShare>1</minShare>
  </pool>
</allocations>
```

**使用方式**：

```scala
// 设置调度池
sc.setLocalProperty("spark.scheduler.pool", "production")

// 运行任务
val result = sc.parallelize(1 to 100).count()

// 重置调度池
sc.setLocalProperty("spark.scheduler.pool", null)
```

### 动态资源分配

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.dynamicAllocation.enabled` | false | 是否启用动态资源分配 |
| `spark.dynamicAllocation.minExecutors` | 0 | 最小 Executor 数量 |
| `spark.dynamicAllocation.maxExecutors` | infinity | 最大 Executor 数量 |
| `spark.dynamicAllocation.initialExecutors` | minExecutors | 初始 Executor 数量 |
| `spark.dynamicAllocation.executorIdleTimeout` | 60s | Executor 空闲超时时间 |
| `spark.dynamicAllocation.schedulerBacklogTimeout` | 1s | 调度积压超时 |

**启用动态分配**：

```properties
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=2
spark.dynamicAllocation.maxExecutors=20
spark.dynamicAllocation.initialExecutors=5

# 需要启用 External Shuffle Service
spark.shuffle.service.enabled=true
```

---

## 安全配置

### 认证

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.authenticate` | false | 是否启用内部认证 |
| `spark.authenticate.secret` | (none) | 认证密钥 |
| `spark.network.crypto.enabled` | false | 是否启用网络加密 |
| `spark.network.crypto.keyLength` | 128 | 加密密钥长度 |

### SSL/TLS

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.ssl.enabled` | false | 是否启用 SSL |
| `spark.ssl.protocol` | TLS | SSL 协议 |
| `spark.ssl.keyStore` | (none) | KeyStore 文件路径 |
| `spark.ssl.keyStorePassword` | (none) | KeyStore 密码 |
| `spark.ssl.trustStore` | (none) | TrustStore 文件路径 |
| `spark.ssl.trustStorePassword` | (none) | TrustStore 密码 |

---

## 监控配置

### Event Log

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.eventLog.enabled` | false | 是否启用事件日志 |
| `spark.eventLog.dir` | file:///tmp/spark-events | 事件日志目录 |
| `spark.eventLog.compress` | false | 是否压缩事件日志 |

**启用 History Server**：

```properties
spark.eventLog.enabled=true
spark.eventLog.dir=hdfs://namenode:8021/spark-logs
spark.eventLog.compress=true
```

### Metrics

**配置文件**：`conf/metrics.properties`

```properties
# 启用 JMX
*.sink.jmx.class=org.apache.spark.metrics.sink.JmxSink

# 启用 Graphite
*.sink.graphite.class=org.apache.spark.metrics.sink.GraphiteSink
*.sink.graphite.host=graphite-server
*.sink.graphite.port=2003
*.sink.graphite.period=10
*.sink.graphite.unit=seconds

# 启用 CSV
*.sink.csv.class=org.apache.spark.metrics.sink.CsvSink
*.sink.csv.directory=/tmp/spark-metrics
*.sink.csv.period=1
*.sink.csv.unit=minutes
```

---

## Spark SQL 配置

### 基本配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.sql.shuffle.partitions` | 200 | SQL Shuffle 分区数 |
| `spark.sql.autoBroadcastJoinThreshold` | 10MB | 自动广播 Join 的阈值 |
| `spark.sql.broadcastTimeout` | 300s | 广播超时时间 |
| `spark.sql.codegen` | true | 是否启用代码生成 |
| `spark.sql.inMemoryColumnarStorage.compressed` | true | 是否压缩列式存储 |
| `spark.sql.inMemoryColumnarStorage.batchSize` | 10000 | 列式存储批次大小 |

### Hive 配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.sql.hive.metastore.version` | 1.2.1 | Hive Metastore 版本 |
| `spark.sql.hive.metastore.jars` | builtin | Metastore JAR 位置 |
| `spark.sql.hive.convertMetastoreParquet` | true | 是否转换 Hive Parquet 表 |
| `spark.sql.hive.convertMetastoreOrc` | true | 是否转换 Hive ORC 表 |

---

## Spark Streaming 配置

### 基本配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.streaming.blockInterval` | 200ms | 接收器生成块的间隔 |
| `spark.streaming.receiver.maxRate` | not set | 接收器每秒最大接收记录数 |
| `spark.streaming.receiver.writeAheadLog.enable` | false | 是否启用 WAL |
| `spark.streaming.unpersist` | true | 是否自动清理旧 RDD |
| `spark.streaming.stopGracefullyOnShutdown` | false | 是否优雅停止 |
| `spark.streaming.backpressure.enabled` | false | 是否启用反压机制 |

### Kafka 配置

```properties
# Direct 方式
spark.streaming.kafka.maxRatePerPartition=10000

# Receiver 方式
spark.streaming.receiver.maxRate=50000
```

---

## 配置最佳实践

### 小数据集（< 10GB）

```properties
spark.executor.memory=2g
spark.executor.cores=2
spark.executor.instances=5
spark.default.parallelism=20
spark.sql.shuffle.partitions=20
```

### 中等数据集（10GB - 100GB）

```properties
spark.executor.memory=4g
spark.executor.cores=4
spark.executor.instances=10
spark.default.parallelism=80
spark.sql.shuffle.partitions=100
```

### 大数据集（> 100GB）

```properties
spark.executor.memory=8g
spark.executor.cores=5
spark.executor.instances=20
spark.default.parallelism=200
spark.sql.shuffle.partitions=200
spark.shuffle.file.buffer=64k
spark.reducer.maxSizeInFlight=96m
```

---

## 参考资料

- [Spark Configuration 官方文档](http://spark.apache.org/docs/1.6.3/configuration.html)
- [性能调优](Performance-Tuning.md)
- [运维手册](Operations.md)

---

**下一步**：查看 [性能调优](Performance-Tuning.md) 了解如何优化 Spark 应用性能。
