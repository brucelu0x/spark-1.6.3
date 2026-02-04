# Spark 运维手册

本文档提供 Apache Spark 1.6.3 的部署、监控和故障排查指南。

## 目录

- [部署模式](#部署模式)
- [Standalone 集群](#standalone-集群)
- [YARN 部署](#yarn-部署)
- [监控和日志](#监控和日志)
- [故障排查](#故障排查)
- [安全配置](#安全配置)
- [备份和恢复](#备份和恢复)

---

## 部署模式

Spark 支持三种部署模式：

### 1. Local 模式

**用途**：开发和测试

```bash
# 单线程
./bin/spark-shell --master local

# 多线程
./bin/spark-shell --master local[4]

# 使用所有核心
./bin/spark-shell --master local[*]
```

### 2. Standalone 模式

**用途**：简单集群部署

```bash
# 启动 Master
./sbin/start-master.sh

# 启动 Worker
./sbin/start-slave.sh spark://master:7077

# 提交应用
./bin/spark-submit \
  --master spark://master:7077 \
  --executor-memory 4G \
  --total-executor-cores 20 \
  myapp.jar
```

### 3. YARN 模式

**用途**：与 Hadoop 集成

```bash
# Client 模式（Driver 在本地）
./bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 4G \
  --num-executors 10 \
  myapp.jar

# Cluster 模式（Driver 在集群）
./bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 4G \
  --num-executors 10 \
  myapp.jar
```

---

## Standalone 集群

### 集群架构

```
┌─────────────┐
│   Master    │ (管理节点)
└──────┬──────┘
       │
   ┌───┴───┬───────┬───────┐
   │       │       │       │
┌──▼───┐ ┌─▼────┐ ┌─▼────┐ ┌─▼────┐
│Worker│ │Worker│ │Worker│ │Worker│
└──────┘ └──────┘ └──────┘ └──────┘
```

### 部署步骤

#### 1. 配置环境变量

编辑 `conf/spark-env.sh`：

```bash
# Master 配置
export SPARK_MASTER_HOST=master
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080

# Worker 配置
export SPARK_WORKER_CORES=8
export SPARK_WORKER_MEMORY=16g
export SPARK_WORKER_PORT=7078
export SPARK_WORKER_WEBUI_PORT=8081

# 日志目录
export SPARK_LOG_DIR=/var/log/spark

# PID 目录
export SPARK_PID_DIR=/var/run/spark
```

#### 2. 配置 Workers

编辑 `conf/slaves`：

```
worker1
worker2
worker3
worker4
```

#### 3. 分发配置

```bash
# 同步到所有节点
for host in worker1 worker2 worker3 worker4; do
  rsync -av /opt/spark/ $host:/opt/spark/
done
```

#### 4. 启动集群

```bash
# 启动 Master 和所有 Workers
./sbin/start-all.sh

# 或者分别启动
./sbin/start-master.sh
./sbin/start-slaves.sh
```

#### 5. 验证集群

访问 Master Web UI：`http://master:8080`

### 高可用配置

使用 ZooKeeper 实现 Master 高可用：

**配置 `conf/spark-env.sh`**：

```bash
export SPARK_DAEMON_JAVA_OPTS="-Dspark.deploy.recoveryMode=ZOOKEEPER \
  -Dspark.deploy.zookeeper.url=zk1:2181,zk2:2181,zk3:2181 \
  -Dspark.deploy.zookeeper.dir=/spark"
```

**启动多个 Master**：

```bash
# 在 master1 上
./sbin/start-master.sh

# 在 master2 上
./sbin/start-master.sh
```

---

## YARN 部署

### 配置 Hadoop 集成

#### 1. 设置 HADOOP_CONF_DIR

编辑 `conf/spark-env.sh`：

```bash
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
```

#### 2. 配置 Spark 属性

编辑 `conf/spark-defaults.conf`：

```properties
spark.master=yarn
spark.submit.deployMode=client
spark.yarn.am.memory=1g
spark.yarn.am.cores=1
spark.executor.memory=4g
spark.executor.cores=4
spark.executor.instances=10
```

### 动态资源分配

启用动态资源分配：

```properties
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=2
spark.dynamicAllocation.maxExecutors=20
spark.dynamicAllocation.initialExecutors=5
spark.shuffle.service.enabled=true
```

**启动 Shuffle Service**：

在每个 NodeManager 上配置 `yarn-site.xml`：

```xml
<property>
  <name>yarn.nodemanager.aux-services</name>
  <value>mapreduce_shuffle,spark_shuffle</value>
</property>

<property>
  <name>yarn.nodemanager.aux-services.spark_shuffle.class</name>
  <value>org.apache.spark.network.yarn.YarnShuffleService</value>
</property>
```

重启 NodeManager：

```bash
yarn-daemon.sh stop nodemanager
yarn-daemon.sh start nodemanager
```

---

## 监控和日志

### Spark UI

**访问方式**：

- **运行中的应用**：`http://driver:4040`
- **History Server**：`http://history-server:18080`

**主要页面**：

1. **Jobs**：查看 Job 列表和执行时间
2. **Stages**：查看 Stage 详情和 Task 分布
3. **Storage**：查看 RDD 缓存情况
4. **Environment**：查看配置信息
5. **Executors**：查看 Executor 资源使用
6. **SQL**：查看 SQL 查询计划

### History Server

#### 1. 配置 Event Log

编辑 `conf/spark-defaults.conf`：

```properties
spark.eventLog.enabled=true
spark.eventLog.dir=hdfs://namenode:8021/spark-logs
spark.eventLog.compress=true
```

#### 2. 启动 History Server

编辑 `conf/spark-defaults.conf`：

```properties
spark.history.fs.logDirectory=hdfs://namenode:8021/spark-logs
spark.history.ui.port=18080
```

启动服务：

```bash
./sbin/start-history-server.sh
```

访问：`http://history-server:18080`

### 日志配置

编辑 `conf/log4j.properties`：

```properties
# 设置日志级别
log4j.rootCategory=INFO, console

# Console 输出
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

# 文件输出
log4j.appender.file=org.apache.log4j.RollingFileAppender
log4j.appender.file.File=/var/log/spark/spark.log
log4j.appender.file.MaxFileSize=100MB
log4j.appender.file.MaxBackupIndex=10
log4j.appender.file.layout=org.apache.log4j.PatternLayout
log4j.appender.file.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

# 设置特定包的日志级别
log4j.logger.org.apache.spark.repl.Main=WARN
log4j.logger.org.spark_project.jetty=WARN
log4j.logger.org.spark_project.jetty.util.component.AbstractLifeCycle=ERROR
log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper=INFO
log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter=INFO
log4j.logger.org.apache.parquet=ERROR
log4j.logger.parquet=ERROR
log4j.logger.org.apache.hadoop.hive.metastore.RetryingHMSHandler=FATAL
log4j.logger.org.apache.hadoop.hive.ql.exec.FunctionRegistry=ERROR
```

### Metrics 监控

编辑 `conf/metrics.properties`：

```properties
# 启用 JMX
*.sink.jmx.class=org.apache.spark.metrics.sink.JmxSink

# 启用 Graphite
*.sink.graphite.class=org.apache.spark.metrics.sink.GraphiteSink
*.sink.graphite.host=graphite-server
*.sink.graphite.port=2003
*.sink.graphite.period=10
*.sink.graphite.unit=seconds
*.sink.graphite.prefix=spark

# 启用 CSV
*.sink.csv.class=org.apache.spark.metrics.sink.CsvSink
*.sink.csv.directory=/tmp/spark-metrics
*.sink.csv.period=1
*.sink.csv.unit=minutes

# 配置 Source
master.source.jvm.class=org.apache.spark.metrics.source.JvmSource
worker.source.jvm.class=org.apache.spark.metrics.source.JvmSource
driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource
executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource
```

---

## 故障排查

### 常见问题

#### 1. OOM (Out of Memory)

**症状**：

```
java.lang.OutOfMemoryError: Java heap space
```

**解决方案**：

```bash
# 增加 Executor 内存
--executor-memory 8G

# 增加 Driver 内存
--driver-memory 4G

# 增加分区数
--conf spark.sql.shuffle.partitions=400
```

#### 2. Shuffle Fetch Failed

**症状**：

```
org.apache.spark.shuffle.FetchFailedException
```

**解决方案**：

```properties
# 增加重试次数
spark.shuffle.io.maxRetries=5
spark.shuffle.io.retryWait=10s

# 增加网络超时
spark.network.timeout=300s

# 增加 Shuffle 缓冲区
spark.shuffle.file.buffer=64k
```

#### 3. Task 超时

**症状**：

```
Task ... failed after ... attempts
```

**解决方案**：

```properties
# 增加 Task 超时时间
spark.network.timeout=600s

# 增加心跳间隔
spark.executor.heartbeatInterval=20s
```

#### 4. 数据倾斜

**症状**：

- 某些 Task 执行时间远超其他 Task
- Shuffle Read 数据量分布不均

**解决方案**：

参考 [性能调优 - 数据倾斜处理](Performance-Tuning.md#数据倾斜处理)

#### 5. GC 频繁

**症状**：

```
GC time占比 > 10%
```

**解决方案**：

```bash
# 使用 G1GC
--conf spark.executor.extraJavaOptions="-XX:+UseG1GC"

# 增加内存
--executor-memory 8G

# 减少缓存
rdd.unpersist()
```

### 日志查看

**Driver 日志**：

```bash
# Standalone 模式
tail -f /var/log/spark/spark-*-driver-*.log

# YARN 模式
yarn logs -applicationId application_xxx
```

**Executor 日志**：

```bash
# Standalone 模式
tail -f /var/log/spark/spark-*-executor-*.log

# YARN 模式
yarn logs -applicationId application_xxx -containerId container_xxx
```

**Master/Worker 日志**：

```bash
tail -f /var/log/spark/spark-*-master-*.log
tail -f /var/log/spark/spark-*-worker-*.log
```

---

## 安全配置

### 认证

启用内部认证：

```properties
spark.authenticate=true
spark.authenticate.secret=my-secret-key
```

### 加密

启用网络加密：

```properties
spark.network.crypto.enabled=true
spark.network.crypto.keyLength=128
spark.network.crypto.keyFactoryAlgorithm=PBKDF2WithHmacSHA1
```

### SSL/TLS

启用 SSL：

```properties
spark.ssl.enabled=true
spark.ssl.protocol=TLS
spark.ssl.keyStore=/path/to/keystore.jks
spark.ssl.keyStorePassword=password
spark.ssl.trustStore=/path/to/truststore.jks
spark.ssl.trustStorePassword=password
```

---

## 备份和恢复

### Checkpoint

启用 Checkpoint：

```scala
// 设置 Checkpoint 目录
sc.setCheckpointDir("hdfs://namenode:8021/spark-checkpoint")

// Checkpoint RDD
rdd.checkpoint()
```

### Streaming Checkpoint

```scala
val ssc = StreamingContext.getOrCreate(checkpointDir, createContext)

def createContext(): StreamingContext = {
  val ssc = new StreamingContext(conf, Seconds(5))
  ssc.checkpoint(checkpointDir)
  // 定义 Streaming 逻辑
  ssc
}
```

---

## 集群管理脚本

### 启动/停止脚本

```bash
# 启动所有服务
./sbin/start-all.sh

# 停止所有服务
./sbin/stop-all.sh

# 启动 Master
./sbin/start-master.sh

# 停止 Master
./sbin/stop-master.sh

# 启动 Workers
./sbin/start-slaves.sh

# 停止 Workers
./sbin/stop-slaves.sh

# 启动 History Server
./sbin/start-history-server.sh

# 停止 History Server
./sbin/stop-history-server.sh

# 启动 Thrift Server
./sbin/start-thriftserver.sh

# 停止 Thrift Server
./sbin/stop-thriftserver.sh
```

### 健康检查脚本

```bash
#!/bin/bash
# check-spark-cluster.sh

# 检查 Master
if curl -s http://master:8080 > /dev/null; then
  echo "Master is running"
else
  echo "Master is down"
  exit 1
fi

# 检查 Workers
for worker in worker1 worker2 worker3 worker4; do
  if curl -s http://$worker:8081 > /dev/null; then
    echo "$worker is running"
  else
    echo "$worker is down"
  fi
done
```

---

## 参考资料

- [Spark Cluster Overview](http://spark.apache.org/docs/1.6.3/cluster-overview.html)
- [Spark Standalone Mode](http://spark.apache.org/docs/1.6.3/spark-standalone.html)
- [Running on YARN](http://spark.apache.org/docs/1.6.3/running-on-yarn.html)
- [Monitoring and Instrumentation](http://spark.apache.org/docs/1.6.3/monitoring.html)

---

**下一步**：查看 [FAQ 和最佳实践](FAQ.md) 了解常见问题和解决方案。
