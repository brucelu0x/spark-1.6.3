# 开发者资源 - Spark 源码阅读指南

本文档提供 Apache Spark 1.6.3 的深度源码阅读指南，帮助开发者理解 Spark 的内部实现。

## 目录

- [源码阅读准备](#源码阅读准备)
- [核心入口类](#核心入口类)
- [RDD 抽象层](#rdd-抽象层)
- [调度系统](#调度系统)
- [执行引擎](#执行引擎)
- [Shuffle 机制](#shuffle-机制)
- [存储系统](#存储系统)
- [网络通信](#网络通信)
- [源码阅读顺序](#源码阅读顺序)
- [调用关系图](#调用关系图)

---

## 源码阅读准备

### 环境要求

- **IDE**: IntelliJ IDEA (推荐) 或 Eclipse
- **JDK**: 1.8+
- **Scala**: 2.10.5
- **构建工具**: Maven 3.x 或 SBT

### 项目导入

参考 [首页](Home.md) 的快速开始部分导入项目。

### 源码目录结构

```
core/src/main/scala/org/apache/spark/
├── SparkContext.scala          # 应用入口
├── SparkEnv.scala              # 运行环境
├── rdd/                        # RDD 实现
│   ├── RDD.scala              # RDD 基类
│   ├── MapPartitionsRDD.scala
│   └── ShuffledRDD.scala
├── scheduler/                  # 调度系统
│   ├── DAGScheduler.scala     # DAG 调度器
│   ├── TaskScheduler.scala    # 任务调度器
│   ├── Task.scala             # Task 抽象
│   ├── ShuffleMapTask.scala
│   └── ResultTask.scala
├── executor/                   # 执行器
│   └── Executor.scala
├── shuffle/                    # Shuffle 系统
│   ├── ShuffleManager.scala
│   └── sort/
│       └── SortShuffleManager.scala
├── storage/                    # 存储系统
│   ├── BlockManager.scala
│   ├── MemoryStore.scala
│   └── DiskStore.scala
└── network/                    # 网络通信
    └── netty/
```

---

## 核心入口类

### 1. SparkContext - 应用程序入口

**文件路径**: `core/src/main/scala/org/apache/spark/SparkContext.scala`

**核心职责**:
- Spark 应用程序的主入口点
- 代表与 Spark 集群的连接
- 用于创建 RDD、累加器和广播变量

**初始化流程** (行 396-596):

```scala
class SparkContext(config: SparkConf) {
  // 1. 配置验证 (行 397-417)
  // 验证 master URL 和应用名称

  // 2. 环境准备 (行 419-448)
  // 设置 driver host/port，处理 jars 和 files

  // 3. 创建 SparkEnv (行 457)
  _env = createSparkEnv(_conf, isLocal, listenerBus)
  SparkEnv.set(_env)

  // 4. 初始化调度器 (行 522-530)
  val (sched, ts) = SparkContext.createTaskScheduler(this, master)
  _schedulerBackend = sched
  _taskScheduler = ts
  _dagScheduler = new DAGScheduler(this)
  _taskScheduler.start()

  // 5. 启动辅助服务 (行 540-576)
  // MetricsSystem, EventLogger, ExecutorAllocationManager, ContextCleaner
}
```

**关键成员变量**:

```scala
private var _env: SparkEnv                    // Spark 运行环境
private var _dagScheduler: DAGScheduler       // DAG 调度器
private var _taskScheduler: TaskScheduler     // 任务调度器
private var _schedulerBackend: SchedulerBackend // 调度后端
```

**常用方法**:

```scala
// 创建 RDD
def parallelize[T](seq: Seq[T], numSlices: Int): RDD[T]
def textFile(path: String, minPartitions: Int): RDD[String]

// 广播变量和累加器
def broadcast[T](value: T): Broadcast[T]
def accumulator[T](initialValue: T): Accumulator[T]

// 提交 Job
def runJob[T, U](rdd: RDD[T], func: Iterator[T] => U): Array[U]
```

### 2. SparkConf - 配置管理

**文件路径**: `core/src/main/scala/org/apache/spark/SparkConf.scala`

**核心功能**:
- 使用 `ConcurrentHashMap` 存储配置键值对
- 支持链式调用设置配置
- 从系统属性加载 `spark.*` 配置

**示例**:

```scala
val conf = new SparkConf()
  .setAppName("MyApp")
  .setMaster("local[*]")
  .set("spark.executor.memory", "4g")
  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

### 3. SparkEnv - 运行环境

**文件路径**: `core/src/main/scala/org/apache/spark/SparkEnv.scala`

**核心组件** (行 56-73):

```scala
class SparkEnv (
    val executorId: String,
    val rpcEnv: RpcEnv,                          // RPC 环境
    val serializer: Serializer,                  // 序列化器
    val closureSerializer: Serializer,           // 闭包序列化器
    val cacheManager: CacheManager,              // 缓存管理器
    val mapOutputTracker: MapOutputTracker,      // Map 输出跟踪器
    val shuffleManager: ShuffleManager,          // Shuffle 管理器
    val broadcastManager: BroadcastManager,      // 广播管理器
    val blockTransferService: BlockTransferService, // 块传输服务
    val blockManager: BlockManager,              // 块管理器
    val securityManager: SecurityManager,        // 安全管理器
    val metricsSystem: MetricsSystem,            // 指标系统
    val memoryManager: MemoryManager,            // 内存管理器
    val outputCommitCoordinator: OutputCommitCoordinator
)
```

**创建方式**:
- Driver 端: `SparkEnv.createDriverEnv()`
- Executor 端: `SparkEnv.createExecutorEnv()`

---

## RDD 抽象层

### 1. RDD 基类设计

**文件路径**: `core/src/main/scala/org/apache/spark/rdd/RDD.scala`

**RDD 五大核心属性** (行 59-66):

```scala
abstract class RDD[T] {
  // 1. 分区列表
  protected def getPartitions: Array[Partition]

  // 2. 计算函数
  def compute(split: Partition, context: TaskContext): Iterator[T]

  // 3. 依赖关系
  protected def getDependencies: Seq[Dependency[_]] = deps

  // 4. 分区器（可选）
  @transient val partitioner: Option[Partitioner] = None

  // 5. 优先位置（可选）
  protected def getPreferredLocations(split: Partition): Seq[String] = Nil
}
```

**Transformation 操作** (返回新 RDD):

```scala
// map: 转换每个元素
def map[U](f: T => U): RDD[U]

// filter: 过滤元素
def filter(f: T => Boolean): RDD[T]

// flatMap: 扁平化映射
def flatMap[U](f: T => TraversableOnce[U]): RDD[U]

// mapPartitions: 按分区处理
def mapPartitions[U](f: Iterator[T] => Iterator[U]): RDD[U]

// reduceByKey: 按 Key 聚合
def reduceByKey(func: (V, V) => V): RDD[(K, V)]
```

**Action 操作** (触发计算):

```scala
// count: 计数
def count(): Long

// collect: 收集到 Driver
def collect(): Array[T]

// reduce: 聚合
def reduce(f: (T, T) => T): T

// foreach: 遍历
def foreach(f: T => Unit): Unit

// saveAsTextFile: 保存为文本文件
def saveAsTextFile(path: String): Unit
```

### 2. 常用 RDD 实现

#### MapPartitionsRDD

**文件路径**: `core/src/main/scala/org/apache/spark/rdd/MapPartitionsRDD.scala`

**特点**:
- 对父 RDD 的每个分区应用函数
- 支持保留分区器 (`preservesPartitioning`)
- 最常用的 RDD 实现，map/filter 等操作都基于此

**核心实现**:

```scala
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false)
  extends RDD[U](prev) {

  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    f(context, split.index, firstParent[T].iterator(split, context))

  override def getPartitions: Array[Partition] = firstParent[T].partitions
}
```

#### ShuffledRDD

**文件路径**: `core/src/main/scala/org/apache/spark/rdd/ShuffledRDD.scala`

**特点**:
- Shuffle 操作的结果 RDD
- 创建 ShuffleDependency 依赖
- 支持设置序列化器、聚合器、排序等

**核心实现**:

```scala
class ShuffledRDD[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient var prev: RDD[_ <: Product2[K, V]],
    part: Partitioner)
  extends RDD[(K, C)](prev.context, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    SparkEnv.get.shuffleManager.getReader(
      dep.shuffleHandle, split.index, split.index + 1, context
    ).read().asInstanceOf[Iterator[(K, C)]]
  }
}
```

### 3. 依赖关系 (Dependency)

**文件路径**: `core/src/main/scala/org/apache/spark/Dependency.scala`

**依赖类型**:

#### 窄依赖 (NarrowDependency)

```scala
// 一对一依赖 (如 map)
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int): List[Int] = List(partitionId)
}

// 范围依赖 (如 union)
class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
  extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int): Seq[Int] = {
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
  }
}
```

**特点**: 父 RDD 的每个分区最多被子 RDD 的一个分区使用

#### 宽依赖 (ShuffleDependency)

```scala
class ShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient private val _rdd: RDD[_ <: Product2[K, V]],
    val partitioner: Partitioner,
    val serializer: Option[Serializer] = None,
    val keyOrdering: Option[Ordering[K]] = None,
    val aggregator: Option[Aggregator[K, V, C]] = None,
    val mapSideCombine: Boolean = false)
  extends Dependency[Product2[K, V]] {

  val shuffleId: Int = _rdd.context.newShuffleId()
  val shuffleHandle: ShuffleHandle = _rdd.context.env.shuffleManager.registerShuffle(
    shuffleId, _rdd.partitions.length, this)
}
```

**特点**: 需要 Shuffle 操作，父 RDD 的每个分区被多个子 RDD 分区使用

---

## 调度系统

### 1. DAGScheduler - 高层调度器

**文件路径**: `core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala`

**核心职责** (行 45-109):
- 将 Job 划分为 Stage 的 DAG
- 跟踪 RDD 和 Stage 输出的物化状态
- 找到最小调度方案运行 Job
- 提交 Stage 作为 TaskSet 给 TaskScheduler

**关键概念**:
- **Job**: 用户提交的作业 (如 count() 触发)
- **Stage**: 任务集合，在 Shuffle 边界划分
  - `ShuffleMapStage`: 写 Shuffle 输出文件
  - `ResultStage`: 执行 Action 的最终 Stage
- **Task**: 单个工作单元

**Stage 划分机制**:

```
RDD Chain:
RDD1 -> map -> RDD2 -> reduceByKey -> RDD3 -> map -> RDD4 -> collect

Stage 划分:
Stage 0: RDD1 -> map -> RDD2 (ShuffleMapStage)
         ↓ Shuffle
Stage 1: RDD3 -> map -> RDD4 (ResultStage)
```

**核心数据结构** (行 139-162):

```scala
private[scheduler] val jobIdToStageIds = new HashMap[Int, HashSet[Int]]
private[scheduler] val stageIdToStage = new HashMap[Int, Stage]
private[scheduler] val shuffleToMapStage = new HashMap[Int, ShuffleMapStage]
private[scheduler] val jobIdToActiveJob = new HashMap[Int, ActiveJob]
private[scheduler] val waitingStages = new HashSet[Stage]    // 等待运行的 Stage
private[scheduler] val runningStages = new HashSet[Stage]    // 正在运行的 Stage
private[scheduler] val failedStages = new HashSet[Stage]     // 失败的 Stage
```

**重要方法**:

```scala
// 提交 Job
def submitJob[T, U](
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int],
    callSite: CallSite,
    resultHandler: (Int, U) => Unit,
    properties: Properties): JobWaiter[U]

// 处理 Job 提交事件
private[scheduler] def handleJobSubmitted(
    jobId: Int,
    finalRDD: RDD[_],
    func: (TaskContext, Iterator[_]) => _,
    partitions: Array[Int],
    callSite: CallSite,
    listener: JobListener,
    properties: Properties)

// 提交 Stage
private def submitStage(stage: Stage)

// 提交缺失的 Task
private def submitMissingTasks(stage: Stage, jobId: Int)
```

### 2. TaskScheduler - 任务调度器

**文件路径**: `core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala`

**核心职责** (行 40-53):
- 通过 SchedulerBackend 为多种集群类型调度任务
- 处理通用逻辑：确定调度顺序、启动推测执行等
- 支持 FIFO 和 FAIR 调度模式

**调度流程**:

```
1. DAGScheduler.submitTasks(taskSet)
   ↓
2. TaskScheduler.submitTasks(taskSet)
   ↓
3. 创建 TaskSetManager
   ↓
4. SchedulerBackend.reviveOffers()
   ↓
5. TaskScheduler.resourceOffers(offers)
   ↓
6. 根据数据本地性分配 Task
   ↓
7. SchedulerBackend.launchTasks(tasks)
```

**数据本地性级别** (TaskLocality):

```scala
object TaskLocality extends Enumeration {
  val PROCESS_LOCAL = Value  // 数据在同一 JVM 进程
  val NODE_LOCAL = Value     // 数据在同一节点
  val NO_PREF = Value        // 无偏好
  val RACK_LOCAL = Value     // 数据在同一机架
  val ANY = Value            // 任意位置
}
```

**关键组件**:
- `TaskSetManager`: 管理单个 TaskSet 的调度
- `Pool`: 调度池，支持层级调度
- `SchedulableBuilder`: 构建调度池 (FIFO/FAIR)

### 3. Stage 类型

#### Stage 基类

**文件路径**: `core/src/main/scala/org/apache/spark/scheduler/Stage.scala`

```scala
private[spark] abstract class Stage(
    val id: Int,
    val rdd: RDD[_],
    val numTasks: Int,
    val parents: List[Stage],
    val firstJobId: Int,
    val callSite: CallSite)
```

#### ShuffleMapStage

**文件路径**: `core/src/main/scala/org/apache/spark/scheduler/ShuffleMapStage.scala`

- 中间 Stage，输出供下游 Stage 使用
- 每个 Task 生成 MapStatus (记录输出位置和大小)

#### ResultStage

**文件路径**: `core/src/main/scala/org/apache/spark/scheduler/ResultStage.scala`

- 最终 Stage，直接计算 Action 结果
- 每个 Task 执行用户函数并返回结果

---

## 执行引擎

### 1. Executor - 执行器

**文件路径**: `core/src/main/scala/org/apache/spark/executor/Executor.scala`

**核心职责** (行 40-52):
- 使用线程池运行 Task
- 支持 Mesos、YARN、Standalone 调度器
- 通过 RPC 与 Driver 通信

**关键组件**:

```scala
// 执行 Task 的线程池
private val threadPool = ThreadUtils.newDaemonCachedThreadPool("Executor task launch worker")

// 正在运行的 Task 映射
private val runningTasks = new ConcurrentHashMap[Long, TaskRunner]

// 心跳线程，定期向 Driver 报告状态
private val heartbeater = new Heartbeater(...)
```

**Task 启动流程** (行 132-142):

```scala
def launchTask(
    context: ExecutorBackend,
    taskId: Long,
    attemptNumber: Int,
    taskName: String,
    serializedTask: ByteBuffer): Unit = {
  val tr = new TaskRunner(context, taskId, attemptNumber, taskName, serializedTask)
  runningTasks.put(taskId, tr)
  threadPool.execute(tr)
}
```

### 2. TaskRunner - 任务运行器

**位置**: Executor.scala 内部类 (行 166-249)

**执行流程** (run 方法):

```scala
override def run(): Unit = {
  // 1. 反序列化 Task 及其依赖
  val (taskFiles, taskJars, taskBytes) = Task.deserializeWithDependencies(serializedTask)

  // 2. 更新依赖
  updateDependencies(taskFiles, taskJars)

  // 3. 反序列化 Task
  val task = ser.deserialize[Task[Any]](taskBytes, Thread.currentThread.getContextClassLoader)

  // 4. 运行 Task
  val value = task.run(taskAttemptId, attemptNumber)

  // 5. 序列化结果
  val valueBytes = ser.serialize(value)

  // 6. 返回结果
  if (valueBytes.limit > maxResultSize) {
    // 大结果存入 BlockManager
    val blockId = TaskResultBlockId(taskId)
    env.blockManager.putBytes(blockId, valueBytes, StorageLevel.MEMORY_AND_DISK_SER)
    IndirectTaskResult[Any](blockId, valueBytes.limit)
  } else {
    // 小结果直接返回
    DirectTaskResult(valueBytes, accumUpdates, task.metrics.get())
  }
}
```

### 3. Task - 任务抽象

**文件路径**: `core/src/main/scala/org/apache/spark/scheduler/Task.scala`

**Task 类型**:

#### ShuffleMapTask

**文件路径**: `core/src/main/scala/org/apache/spark/scheduler/ShuffleMapTask.scala`

```scala
private[spark] class ShuffleMapTask(
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation],
    internalAccumulators: Seq[Accumulator[Long]])
  extends Task[MapStatus](stageId, stageAttemptId, partition.index, internalAccumulators) {

  override def runTask(context: TaskContext): MapStatus = {
    // 反序列化 RDD 和 ShuffleDependency
    val (rdd, dep) = deserialize(taskBinary)

    // 获取 ShuffleWriter
    val writer = SparkEnv.get.shuffleManager.getWriter[Any, Any](
      dep.shuffleHandle, partitionId, context)

    // 写入 Shuffle 数据
    writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])

    // 停止并返回 MapStatus
    writer.stop(success = true).get
  }
}
```

#### ResultTask

**文件路径**: `core/src/main/scala/org/apache/spark/scheduler/ResultTask.scala`

```scala
private[spark] class ResultTask[T, U](
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    locs: Seq[TaskLocation],
    val outputId: Int,
    internalAccumulators: Seq[Accumulator[Long]])
  extends Task[U](stageId, stageAttemptId, partition.index, internalAccumulators) {

  override def runTask(context: TaskContext): U = {
    // 反序列化 RDD 和函数
    val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)

    // 执行函数
    func(context, rdd.iterator(partition, context))
  }
}
```

---

继续阅读 [开发者资源 - 第二部分](Developer-Resources-Part2.md) 了解 Shuffle 机制、存储系统和网络通信。
