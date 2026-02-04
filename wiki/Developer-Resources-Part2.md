# 开发者资源 - 第二部分

本文档是 [开发者资源](Developer-Resources.md) 的续篇，涵盖 Shuffle 机制、存储系统、网络通信和源码阅读顺序。

## Shuffle 机制

### 1. ShuffleManager - Shuffle 管理器

**文件路径**: `core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala`

**接口定义**:

```scala
private[spark] trait ShuffleManager {
  def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle

  def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Int,
      context: TaskContext): ShuffleWriter[K, V]

  def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C]

  def unregisterShuffle(shuffleId: Int): Boolean
  def shuffleBlockResolver: ShuffleBlockResolver
  def stop(): Unit
}
```

### 2. SortShuffleManager - Sort-based Shuffle

**文件路径**: `core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleManager.scala`

**三种 Shuffle 写路径** (行 33-67):

#### 1) BypassMergeSortShuffleWriter

**条件**:
- 分区数 < `spark.shuffle.sort.bypassMergeThreshold` (默认 200)
- 无需聚合

**特点**:
- 直接为每个分区写文件，最后合并
- 避免排序开销

#### 2) UnsafeShuffleWriter (序列化排序)

**条件**:
- 无聚合
- 无排序
- 序列化器支持重定位
- 分区数 < 16777216

**特点**:
- 直接对序列化数据排序
- 减少内存和 GC 开销
- 使用 ShuffleExternalSorter 和 PackedRecordPointer

#### 3) SortShuffleWriter (反序列化排序)

**条件**: 默认路径，处理所有其他情况

**特点**:
- 使用 ExternalSorter 进行排序和聚合
- 支持 Map 端聚合

**Writer 选择逻辑** (行 87-107):

```scala
override def registerShuffle[K, V, C](
    shuffleId: Int,
    numMaps: Int,
    dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
  if (SortShuffleWriter.shouldBypassMergeSort(conf, dependency)) {
    new BypassMergeSortShuffleHandle[K, V](shuffleId, numMaps, dependency)
  } else if (SortShuffleManager.canUseSerializedShuffle(dependency)) {
    new SerializedShuffleHandle[K, V](shuffleId, numMaps, dependency)
  } else {
    new BaseShuffleHandle(shuffleId, numMaps, dependency)
  }
}
```

### 3. ShuffleWriter 和 ShuffleReader

**ShuffleWriter**:

```scala
private[spark] abstract class ShuffleWriter[K, V] {
  def write(records: Iterator[Product2[K, V]]): Unit
  def stop(success: Boolean): Option[MapStatus]
}
```

**ShuffleReader** (`BlockStoreShuffleReader.scala`):

```scala
private[spark] class BlockStoreShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext)
  extends ShuffleReader[K, C] {

  override def read(): Iterator[Product2[K, C]] = {
    // 1. 获取 Shuffle 数据
    val blockFetcherItr = new ShuffleBlockFetcherIterator(...)

    // 2. 反序列化
    val serializerInstance = dep.serializer.newInstance()
    val recordIter = blockFetcherItr.flatMap { ... }

    // 3. 聚合（如果需要）
    val aggregatedIter = if (dep.aggregator.isDefined) {
      dep.aggregator.get.combineCombinersByKey(recordIter, context)
    } else {
      recordIter
    }

    // 4. 排序（如果需要）
    val resultIter = if (dep.keyOrdering.isDefined) {
      val sorter = new ExternalSorter[K, C, C](...)
      sorter.insertAll(aggregatedIter)
      sorter.iterator
    } else {
      aggregatedIter
    }

    resultIter
  }
}
```

### 4. IndexShuffleBlockResolver

**文件路径**: `core/src/main/scala/org/apache/spark/shuffle/IndexShuffleBlockResolver.scala`

**职责**:
- 管理 Shuffle 块的存储和检索
- 为每个 Shuffle Map 输出创建索引文件和数据文件
- 索引文件记录每个分区在数据文件中的偏移量

**文件格式**:

```
数据文件: shuffle_${shuffleId}_${mapId}_0.data
索引文件: shuffle_${shuffleId}_${mapId}_0.index

索引文件内容:
[offset_0, offset_1, offset_2, ..., offset_n]

分区 i 的数据范围: [offset_i, offset_{i+1})
```

---

## 存储系统

### 1. BlockManager - 块管理器

**文件路径**: `core/src/main/scala/org/apache/spark/storage/BlockManager.scala`

**核心职责** (行 59-76):
- 在 Driver 和 Executor 上运行
- 提供本地和远程存储块的接口
- 支持内存、磁盘、堆外存储

**核心组件** (行 78-93):

```scala
val diskBlockManager = new DiskBlockManager(this, conf)
private[spark] val memoryStore = new MemoryStore(this, memoryManager)
private[spark] val diskStore = new DiskStore(this, diskBlockManager)
private[spark] lazy val externalBlockStore: ExternalBlockStore
```

**关键方法**:

```scala
// 获取块
def get(blockId: BlockId): Option[BlockResult]

// 存储块
def putBytes(
    blockId: BlockId,
    bytes: ByteBuffer,
    level: StorageLevel,
    tellMaster: Boolean = true): Boolean

// 从远程获取块
def getRemote(blockId: BlockId): Option[BlockResult]

// 删除块
def removeBlock(blockId: BlockId, tellMaster: Boolean = true): Unit
```

**BlockManagerMaster**:
- Driver 端的 BlockManager 主控
- 跟踪所有 Executor 的 BlockManager 状态
- 通过 RPC 与 Executor 的 BlockManager 通信

### 2. MemoryStore - 内存存储

**文件路径**: `core/src/main/scala/org/apache/spark/storage/MemoryStore.scala`

**存储方式**:

```scala
private case class MemoryEntry(
    value: Any,
    size: Long,
    deserialized: Boolean)

private val entries = new LinkedHashMap[BlockId, MemoryEntry](32, 0.75f, true)
```

**Unroll 机制** (行 46-56):
- 逐步展开 Iterator 到内存
- 如果内存不足，可以回退到磁盘
- 使用 `unrollMemoryMap` 跟踪展开内存

**关键方法**:

```scala
// 存储序列化数据
def putBytes(blockId: BlockId, bytes: ByteBuffer, level: StorageLevel): PutResult

// 存储迭代器数据
def putIterator[T](
    blockId: BlockId,
    values: Iterator[T],
    level: StorageLevel,
    returnValues: Boolean): PutResult

// 安全展开迭代器
private def unrollSafely[T](
    blockId: BlockId,
    values: Iterator[T]): Either[Array[Any], Iterator[T]]
```

### 3. DiskStore - 磁盘存储

**文件路径**: `core/src/main/scala/org/apache/spark/storage/DiskStore.scala`

**特点**:
- 使用 DiskBlockManager 管理磁盘文件
- 支持内存映射读取 (mmap)
- 序列化后写入磁盘

**关键方法**:

```scala
// 写入序列化数据
def putBytes(blockId: BlockId, bytes: ByteBuffer): Unit

// 序列化并写入迭代器
def putIterator[T](
    blockId: BlockId,
    values: Iterator[T],
    serializer: Serializer): Unit

// 读取为 ByteBuffer
def getBytes(blockId: BlockId): Option[ByteBuffer]

// 反序列化读取
def getValues[T](blockId: BlockId): Option[Iterator[T]]
```

### 4. CacheManager - 缓存管理器

**文件路径**: `core/src/main/scala/org/apache/spark/CacheManager.scala`

**核心职责** (行 27-29):
- 将 RDD 分区内容传递给 BlockManager
- 确保节点不会加载 RDD 的两个副本

**核心方法** `getOrCompute()` (行 36-91):

```scala
def getOrCompute[T](
    rdd: RDD[T],
    partition: Partition,
    context: TaskContext,
    storageLevel: StorageLevel): Iterator[T] = {

  val key = RDDBlockId(rdd.id, partition.index)

  // 1. 检查 BlockManager 是否已有缓存
  blockManager.get(key) match {
    case Some(blockResult) =>
      // 缓存命中，直接返回
      return blockResult.data.asInstanceOf[Iterator[T]]

    case None =>
      // 缓存未命中，需要计算
  }

  // 2. 获取加载锁
  val loading = new AtomicBoolean(false)
  val loadingEntry = putIfAbsent(key, loading)

  if (loadingEntry != null) {
    // 其他线程正在加载，等待
    while (!loading.get()) {
      Thread.sleep(10)
    }
    return blockManager.get(key).get.data.asInstanceOf[Iterator[T]]
  }

  // 3. 计算分区数据
  val computedValues = rdd.computeOrReadCheckpoint(partition, context)

  // 4. 存储到 BlockManager
  if (storageLevel.useMemory) {
    blockManager.putIterator(key, computedValues, storageLevel, returnValues = true)
  }

  // 5. 返回迭代器
  computedValues
}
```

---

## 网络通信

### 1. NettyBlockTransferService

**文件路径**: `core/src/main/scala/org/apache/spark/network/netty/NettyBlockTransferService.scala`

**核心功能**:
- 使用 Netty 实现块传输服务
- 支持批量获取块
- 支持上传块到远程节点

**关键方法**:

```scala
// 从远程获取块
override def fetchBlocks(
    host: String,
    port: Int,
    execId: String,
    blockIds: Array[String],
    listener: BlockFetchingListener): Unit = {

  val blockFetchStarter = new RetryingBlockFetcher.BlockFetchStarter {
    override def createAndStart(blockIds: Array[String], listener: BlockFetchingListener) {
      val client = clientFactory.createClient(host, port)
      new OneForOneBlockFetcher(client, appId, execId, blockIds, listener).start()
    }
  }

  val maxRetries = conf.getInt("spark.shuffle.io.maxRetries", 3)
  if (maxRetries > 0) {
    new RetryingBlockFetcher(conf, blockFetchStarter, blockIds, listener).start()
  } else {
    blockFetchStarter.createAndStart(blockIds, listener)
  }
}

// 上传块到远程
override def uploadBlock(
    hostname: String,
    port: Int,
    execId: String,
    blockId: BlockId,
    blockData: ManagedBuffer,
    level: StorageLevel): Future[Unit] = {

  val client = clientFactory.createClient(hostname, port)
  val result = Promise[Unit]()

  client.sendRpc(new UploadBlock(appId, execId, blockId.toString, metadata, blockData).toByteBuffer,
    new RpcResponseCallback {
      override def onSuccess(response: ByteBuffer): Unit = {
        result.success(())
      }
      override def onFailure(e: Throwable): Unit = {
        result.failure(e)
      }
    })

  result.future
}
```

### 2. RPC 框架

**文件路径**: `core/src/main/scala/org/apache/spark/rpc/RpcEnv.scala`

**核心概念**:

```scala
// RPC 环境，管理 RpcEndpoint
abstract class RpcEnv(conf: SparkConf) {
  def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef
  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]
  def stop(endpoint: RpcEndpointRef): Unit
  def shutdown(): Unit
  def awaitTermination(): Unit
}

// RPC 端点，接收消息
trait RpcEndpoint {
  def receive: PartialFunction[Any, Unit]
  def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit]
}

// RPC 端点引用，发送消息
abstract class RpcEndpointRef(conf: SparkConf) {
  def send(message: Any): Unit
  def ask[T](message: Any, timeout: RpcTimeout): Future[T]
}
```

**通信模式**:
- `send()`: 单向发送，不等待响应
- `ask()`: 请求-响应模式，等待返回

---

## 源码阅读顺序

### 第一阶段：理解核心概念 (5-7 天)

#### 1. RDD 抽象 (2-3 天)

**阅读顺序**:
1. `RDD.scala` (行 59-66) - 理解 RDD 五大属性
2. `Dependency.scala` - 理解依赖关系
3. `MapPartitionsRDD.scala` - 最简单的 RDD 实现
4. `ShuffledRDD.scala` - Shuffle RDD 实现
5. `ParallelCollectionRDD.scala` - 并行化集合

**重点关注**:
- RDD 的不可变性和惰性求值
- 窄依赖 vs 宽依赖
- Transformation vs Action

#### 2. SparkContext 初始化 (1-2 天)

**阅读顺序**:
1. `SparkConf.scala` - 配置管理
2. `SparkContext.scala` (行 396-596) - 初始化流程
3. `SparkEnv.scala` - 运行环境

**重点关注**:
- SparkContext 的创建过程
- SparkEnv 包含的组件
- Driver 和 Executor 的区别

#### 3. 简单示例运行 (1-2 天)

**实践**:
```scala
val sc = new SparkContext("local[*]", "Test")
val rdd = sc.parallelize(1 to 100)
val result = rdd.map(_ * 2).filter(_ > 50).collect()
```

**调试跟踪**:
- 设置断点在 `SparkContext.parallelize()`
- 跟踪 `map()` 和 `filter()` 如何创建新 RDD
- 跟踪 `collect()` 如何触发 Job

### 第二阶段：调度系统 (5-7 天)

#### 4. DAGScheduler (3-4 天)

**阅读顺序**:
1. `DAGScheduler.scala` (行 44-200) - 核心概念和数据结构
2. `Stage.scala` - Stage 抽象
3. `ShuffleMapStage.scala` / `ResultStage.scala` - Stage 类型
4. `submitJob()` 方法 - Job 提交
5. `handleJobSubmitted()` 方法 - Job 处理
6. `submitStage()` 方法 - Stage 提交
7. `submitMissingTasks()` 方法 - Task 提交

**重点关注**:
- Stage 划分算法
- Shuffle 边界的识别
- Stage 的依赖关系

#### 5. TaskScheduler (2-3 天)

**阅读顺序**:
1. `TaskScheduler.scala` - 接口定义
2. `TaskSchedulerImpl.scala` - 实现
3. `TaskSetManager.scala` - TaskSet 管理
4. `Pool.scala` - 调度池
5. `SchedulableBuilder.scala` - FIFO/FAIR 调度

**重点关注**:
- 数据本地性调度
- 推测执行
- 调度模式 (FIFO vs FAIR)

### 第三阶段：执行引擎 (4-5 天)

#### 6. Task 执行 (2-3 天)

**阅读顺序**:
1. `Task.scala` - Task 抽象
2. `ShuffleMapTask.scala` - Map Task
3. `ResultTask.scala` - Result Task
4. `Executor.scala` - Executor 和 TaskRunner

**重点关注**:
- Task 的序列化和反序列化
- Task 的执行流程
- 结果的返回方式

#### 7. 完整流程跟踪 (2 天)

**实践**:
```scala
val rdd1 = sc.parallelize(1 to 100, 4)
val rdd2 = rdd1.map(_ * 2)
val rdd3 = rdd2.reduceByKey(_ + _)
val result = rdd3.collect()
```

**跟踪点**:
1. `collect()` 触发 Job
2. DAGScheduler 划分 Stage
3. TaskScheduler 调度 Task
4. Executor 执行 Task
5. 结果返回 Driver

### 第四阶段：Shuffle 机制 (5-6 天)

#### 8. Shuffle 系统 (3-4 天)

**阅读顺序**:
1. `ShuffleManager.scala` - 接口
2. `SortShuffleManager.scala` - Sort Shuffle 实现
3. `SortShuffleWriter.scala` - Writer 实现
4. `BlockStoreShuffleReader.scala` - Reader 实现
5. `IndexShuffleBlockResolver.scala` - 块解析器
6. Java 实现: `UnsafeShuffleWriter.java`, `ShuffleExternalSorter.java`

**重点关注**:
- 三种 Shuffle 写路径的选择
- Sort-based Shuffle 的优化
- Shuffle 数据的存储格式

#### 9. ExternalSorter (2 天)

**阅读顺序**:
1. `ExternalSorter.scala` - 外部排序器
2. `Spillable.scala` - 溢写机制
3. `AppendOnlyMap.scala` - 只追加 Map

**重点关注**:
- 内存和磁盘的平衡
- 溢写触发条件
- 合并排序

### 第五阶段：存储系统 (5-6 天)

#### 10. BlockManager (3-4 天)

**阅读顺序**:
1. `BlockManager.scala` - 块管理器核心
2. `BlockManagerMaster.scala` - Master 端
3. `MemoryStore.scala` - 内存存储
4. `DiskStore.scala` - 磁盘存储
5. `CacheManager.scala` - 缓存管理

**重点关注**:
- 块的存储和检索
- 内存和磁盘的协调
- Unroll 机制

#### 11. 内存管理 (2 天)

**阅读顺序**:
1. `MemoryManager.scala` - 内存管理器
2. `UnifiedMemoryManager.scala` - 统一内存管理
3. `StorageMemoryPool.scala` - 存储内存池
4. `ExecutionMemoryPool.scala` - 执行内存池

**重点关注**:
- 统一内存管理模型
- 执行内存和存储内存的动态调整

### 第六阶段：网络通信 (3-4 天)

#### 12. 网络层 (2-3 天)

**阅读顺序**:
1. `NettyBlockTransferService.scala` - Netty 块传输
2. `RpcEnv.scala` - RPC 环境
3. `RpcEndpoint.scala` / `RpcEndpointRef.scala` - RPC 端点
4. `NettyRpcEnv.scala` - Netty RPC 实现

**重点关注**:
- Netty 的使用
- RPC 通信模式
- 块传输优化

#### 13. 序列化 (1 天)

**阅读顺序**:
1. `Serializer.scala` - 序列化器接口
2. `JavaSerializer.scala` - Java 序列化
3. `KryoSerializer.scala` - Kryo 序列化

**重点关注**:
- 序列化器的选择
- Kryo 的优化

---

## 调用关系图

### 完整调用链

```
用户代码: rdd.collect()
    ↓
SparkContext.runJob()
    ↓
DAGScheduler.runJob()
    ↓
DAGScheduler.submitJob()
    ↓
DAGScheduler.handleJobSubmitted()
    ├─> createStages() (划分 Stage)
    │   ├─> getOrCreateShuffleMapStage()
    │   └─> createResultStage()
    ↓
DAGScheduler.submitStage()
    ├─> submitMissingTasks()
    │   ├─> 创建 ShuffleMapTask 或 ResultTask
    │   └─> TaskScheduler.submitTasks()
    ↓
TaskScheduler.submitTasks()
    ├─> 创建 TaskSetManager
    └─> SchedulerBackend.reviveOffers()
        ↓
TaskScheduler.resourceOffers()
    ├─> TaskSetManager.resourceOffer() (选择 Task)
    └─> SchedulerBackend.launchTasks()
        ↓
Executor.launchTask()
    ├─> 创建 TaskRunner
    └─> threadPool.execute(TaskRunner)
        ↓
TaskRunner.run()
    ├─> 反序列化 Task
    ├─> Task.run()
    │   ├─> ShuffleMapTask.runTask()
    │   │   ├─> RDD.iterator()
    │   │   │   └─> RDD.compute()
    │   │   └─> ShuffleWriter.write()
    │   │       └─> IndexShuffleBlockResolver.writeIndexFileAndCommit()
    │   │
    │   └─> ResultTask.runTask()
    │       ├─> RDD.iterator()
    │       │   └─> RDD.compute()
    │       └─> func(context, iterator)
    │
    └─> 序列化结果并返回
```

### RDD 计算链

```
RDD.iterator(partition, context)
    ↓
检查是否有 Checkpoint
    ├─> 有: 读取 Checkpoint 数据
    └─> 无: 继续
        ↓
检查是否有缓存
    ├─> 有: CacheManager.getOrCompute()
    │       └─> BlockManager.get()
    └─> 无: 继续
        ↓
RDD.computeOrReadCheckpoint()
    ↓
RDD.compute(partition, context)
    ├─> MapPartitionsRDD: f(parent.iterator())
    ├─> ShuffledRDD: ShuffleReader.read()
    └─> ParallelCollectionRDD: 返回本地数据
```

### Shuffle 数据流

```
Map 端:
ShuffleMapTask.runTask()
    ↓
ShuffleWriter.write(rdd.iterator())
    ├─> SortShuffleWriter
    │   ├─> ExternalSorter.insertAll()
    │   │   ├─> 内存中排序
    │   │   └─> 溢写到磁盘 (如果需要)
    │   └─> ExternalSorter.writePartitionedFile()
    │       └─> IndexShuffleBlockResolver.writeIndexFileAndCommit()
    │
    ├─> UnsafeShuffleWriter
    │   ├─> ShuffleExternalSorter.insertRecord()
    │   └─> ShuffleExternalSorter.closeAndGetSpills()
    │
    └─> BypassMergeSortShuffleWriter
        ├─> 为每个分区写临时文件
        └─> 合并所有临时文件

Reduce 端:
ShuffledRDD.compute()
    ↓
ShuffleReader.read()
    ├─> ShuffleBlockFetcherIterator (获取数据)
    │   ├─> 本地读取: BlockManager.getBlockData()
    │   └─> 远程读取: NettyBlockTransferService.fetchBlocks()
    ├─> 反序列化
    ├─> 聚合 (如果需要)
    └─> 排序 (如果需要)
```

---

## 学习建议

### 1. 循序渐进

- 不要试图一次理解所有代码
- 先理解核心概念，再深入细节
- 结合实际例子进行学习

### 2. 动手实践

- 编写简单的 Spark 程序
- 使用 IDE 调试跟踪执行流程
- 修改源码进行实验

### 3. 画图总结

- 绘制类图和时序图
- 总结关键流程
- 记录重要发现

### 4. 参考资料

- [Spark 官方文档](http://spark.apache.org/docs/1.6.3/)
- [Spark 源码](https://github.com/apache/spark/tree/v1.6.3)
- [架构与设计](Architecture.md)
- [性能调优](Performance-Tuning.md)

---

**返回**: [开发者资源 - 第一部分](Developer-Resources.md)
