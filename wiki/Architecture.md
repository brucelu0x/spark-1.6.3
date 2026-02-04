# Spark 架构与设计

本文档详细介绍 Apache Spark 1.6.3 的架构设计和核心组件。

## 目录

- [整体架构](#整体架构)
- [核心组件](#核心组件)
- [Spark Core](#spark-core)
- [Spark SQL](#spark-sql)
- [Spark Streaming](#spark-streaming)
- [网络与通信](#网络与通信)
- [外部集成](#外部集成)

---

## 整体架构

### 架构层次

```
┌─────────────────────────────────────────────────────┐
│  应用层：Spark SQL, Streaming, Python/R API         │
├─────────────────────────────────────────────────────┤
│  核心层：RDD API, DAGScheduler, TaskScheduler       │
├─────────────────────────────────────────────────────┤
│  存储层：BlockManager, MemoryStore, DiskStore       │
├─────────────────────────────────────────────────────┤
│  资源管理：Standalone, YARN, Mesos                  │
└─────────────────────────────────────────────────────┘
```

### 运行模式

Spark 支持三种集群管理器：

1. **Standalone**：Spark 自带的简单集群管理器
2. **YARN**：Hadoop 的资源管理器
3. **Mesos**：通用的集群管理器

### 核心概念

- **Driver**：运行 main 函数并创建 SparkContext 的进程
- **Executor**：在 Worker 节点上运行的进程，执行 Task
- **Task**：最小的执行单元，运行在 Executor 上
- **Job**：由 Action 操作触发的计算作业
- **Stage**：Job 的子集，由 Shuffle 边界划分
- **RDD**：弹性分布式数据集，Spark 的基本抽象

---

## 核心组件

### SparkContext

**位置**：`core/src/main/scala/org/apache/spark/SparkContext.scala`

SparkContext 是 Spark 应用的入口点，负责：

- 连接到集群管理器
- 创建 RDD
- 广播变量和累加器
- 提交 Job

**初始化流程**：

```scala
// 1. 创建 SparkConf
val conf = new SparkConf()
  .setAppName("MyApp")
  .setMaster("local[*]")

// 2. 创建 SparkContext
val sc = new SparkContext(conf)

// 3. SparkContext 内部初始化
// - 创建 SparkEnv
// - 创建 DAGScheduler
// - 创建 TaskScheduler
// - 启动 UI
```

### SparkEnv

**位置**：`core/src/main/scala/org/apache/spark/SparkEnv.scala`

SparkEnv 包含 Spark 运行时环境的所有组件：

- **SerializerManager**：序列化管理
- **BlockManager**：块存储管理
- **MapOutputTracker**：Shuffle 输出跟踪
- **ShuffleManager**：Shuffle 管理
- **BroadcastManager**：广播变量管理
- **CacheManager**：缓存管理

---

## Spark Core

### RDD 抽象

**位置**：`core/src/main/scala/org/apache/spark/rdd/RDD.scala`

RDD（Resilient Distributed Dataset）是 Spark 的核心抽象，具有以下特性：

1. **不可变性**：一旦创建不可修改
2. **分区性**：数据分布在多个分区
3. **容错性**：通过 Lineage 重新计算
4. **惰性求值**：只有 Action 操作才触发计算

**RDD 五大属性**：

```scala
abstract class RDD[T] {
  // 1. 分区列表
  def getPartitions: Array[Partition]

  // 2. 计算函数
  def compute(split: Partition, context: TaskContext): Iterator[T]

  // 3. 依赖关系
  def getDependencies: Seq[Dependency[_]]

  // 4. 分区器（可选）
  def partitioner: Option[Partitioner]

  // 5. 优先位置（可选）
  def getPreferredLocations(split: Partition): Seq[String]
}
```

### 依赖关系

**位置**：`core/src/main/scala/org/apache/spark/Dependency.scala`

两种依赖类型：

1. **窄依赖（Narrow Dependency）**
   - 父 RDD 的每个分区最多被一个子 RDD 分区使用
   - 例如：map, filter, union
   - 可以在同一个 Stage 中执行

2. **宽依赖（Wide Dependency）**
   - 父 RDD 的每个分区被多个子 RDD 分区使用
   - 例如：groupByKey, reduceByKey, join
   - 需要 Shuffle，划分新的 Stage

```
窄依赖示例：
RDD1 [P1] [P2] [P3]
      |    |    |
RDD2 [P1] [P2] [P3]

宽依赖示例：
RDD1 [P1] [P2] [P3]
      \   |   /
       \  |  /
RDD2    [P1] [P2]
```

### 调度系统

#### DAGScheduler

**位置**：`core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala`

DAGScheduler 负责：

- 将 Job 划分为 Stage
- 提交 Stage 到 TaskScheduler
- 处理 Shuffle 依赖
- 重新提交失败的 Stage

**Stage 划分算法**：

1. 从最后的 RDD 开始反向遍历
2. 遇到宽依赖（Shuffle）就划分新的 Stage
3. 窄依赖的 RDD 放在同一个 Stage

#### TaskScheduler

**位置**：`core/src/main/scala/org/apache/spark/scheduler/TaskScheduler.scala`

TaskScheduler 负责：

- 将 Task 分配到 Executor
- 处理 Task 失败和重试
- 实现本地性调度

**调度策略**：

- **FIFO**：先进先出（默认）
- **FAIR**：公平调度，支持资源池

### 执行引擎

#### Executor

**位置**：`core/src/main/scala/org/apache/spark/executor/Executor.scala`

Executor 是运行在 Worker 节点上的进程，负责：

- 运行 Task
- 管理内存和磁盘存储
- 向 Driver 报告状态

#### Task

**位置**：`core/src/main/scala/org/apache/spark/scheduler/Task.scala`

两种 Task 类型：

1. **ShuffleMapTask**：输出数据到 Shuffle 系统
2. **ResultTask**：计算最终结果返回给 Driver

### Shuffle 机制

**位置**：`core/src/main/scala/org/apache/spark/shuffle/`

Spark 1.6.3 使用 **Sort-based Shuffle**：

**Shuffle Write 流程**：

1. Map 端对数据按 PartitionId 排序
2. 写入单个文件（减少文件数）
3. 生成索引文件记录每个分区的位置

**Shuffle Read 流程**：

1. Reduce 端根据索引文件读取数据
2. 可以选择是否排序
3. 支持内存和磁盘溢写

**优化点**：

- 减少文件数：N 个 Map Task 只生成 N 个文件
- 支持 Bypass 模式：分区数少时直接写入
- 内存管理：使用 Tungsten 内存管理

### 存储系统

#### BlockManager

**位置**：`core/src/main/scala/org/apache/spark/storage/BlockManager.scala`

BlockManager 负责：

- 管理 RDD 缓存
- 管理 Shuffle 数据
- 管理广播变量

**存储级别**：

```scala
// 仅内存
MEMORY_ONLY

// 内存 + 磁盘
MEMORY_AND_DISK

// 内存 + 序列化
MEMORY_ONLY_SER

// 内存 + 磁盘 + 序列化
MEMORY_AND_DISK_SER

// 磁盘
DISK_ONLY

// 带副本
MEMORY_ONLY_2, MEMORY_AND_DISK_2
```

#### MemoryStore 和 DiskStore

- **MemoryStore**：内存存储，使用 LinkedHashMap
- **DiskStore**：磁盘存储，使用本地文件系统

---

## Spark SQL

### Catalyst 优化器

**位置**：`sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/`

Catalyst 是 Spark SQL 的查询优化器，包含四个阶段：

1. **Analysis**：解析 SQL，生成未解析的逻辑计划
2. **Logical Optimization**：逻辑优化（谓词下推、列裁剪等）
3. **Physical Planning**：生成物理计划
4. **Code Generation**：生成 Java 字节码

**优化规则示例**：

- **Predicate Pushdown**：将过滤条件下推到数据源
- **Column Pruning**：只读取需要的列
- **Constant Folding**：常量折叠
- **Join Reordering**：Join 顺序优化

### DataFrame API

**位置**：`sql/core/src/main/scala/org/apache/spark/sql/DataFrame.scala`

DataFrame 是 Spark SQL 的核心抽象：

```scala
// 创建 DataFrame
val df = sqlContext.read.json("people.json")

// 操作
df.select("name", "age")
  .filter($"age" > 21)
  .groupBy("age")
  .count()
  .show()
```

### Hive 集成

**位置**：`sql/hive/src/main/scala/org/apache/spark/sql/hive/`

Spark SQL 可以读取 Hive 表：

- 支持 HiveQL
- 支持 Hive UDF
- 支持 Hive SerDe
- 兼容 Hive Metastore

---

## Spark Streaming

**位置**：`streaming/src/main/scala/org/apache/spark/streaming/`

### DStream 抽象

DStream（Discretized Stream）是 Spark Streaming 的核心抽象：

- 表示连续的数据流
- 内部由一系列 RDD 组成
- 支持窗口操作

### 微批处理模型

```
Time: 0s    1s    2s    3s    4s
      |-----|-----|-----|-----|
Data: [RDD1][RDD2][RDD3][RDD4]
```

每个批次生成一个 RDD，然后进行处理。

### 容错机制

- **Checkpoint**：定期保存状态到 HDFS
- **WAL**：Write-Ahead Log，记录接收的数据
- **Receiver**：可靠接收器保证数据不丢失

---

## 网络与通信

### Netty 网络框架

**位置**：`network/common/src/main/java/org/apache/spark/network/`

Spark 使用 Netty 进行网络通信：

- **TransportClient**：客户端
- **TransportServer**：服务端
- **TransportContext**：传输上下文

### RPC 框架

**位置**：`core/src/main/scala/org/apache/spark/rpc/`

Spark 1.6.3 使用 Akka 作为 RPC 框架（后续版本改为 Netty RPC）：

- Driver 和 Executor 之间的通信
- Master 和 Worker 之间的通信

---

## 外部集成

### Kafka 集成

**位置**：`external/kafka/src/main/scala/org/apache/spark/streaming/kafka/`

支持两种方式：

1. **Receiver-based**：使用 Kafka High Level Consumer
2. **Direct**：直接读取 Kafka 分区（推荐）

### Flume 集成

**位置**：`external/flume/src/main/scala/org/apache/spark/streaming/flume/`

支持两种方式：

1. **Push-based**：Flume 推送数据到 Spark
2. **Pull-based**：Spark 从 Flume 拉取数据

---

## 模块依赖关系

```
┌──────────────┐
│  Streaming   │
└──────┬───────┘
       │
┌──────▼───────┐     ┌──────────┐
│   SQL/Core   │────▶│   Hive   │
└──────┬───────┘     └──────────┘
       │
┌──────▼───────┐
│     Core     │
└──────┬───────┘
       │
┌──────▼───────┐
│    Unsafe    │
└──────────────┘
```

---

## 参考资料

- [Spark 官方文档](http://spark.apache.org/docs/1.6.3/)
- [Spark 源码](https://github.com/apache/spark/tree/v1.6.3)
- [开发者资源](Developer-Resources.md) - 深入源码阅读指南

---

**下一步**：查看 [配置参考](Configuration.md) 了解如何配置 Spark。
