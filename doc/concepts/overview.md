# 概念总览

理解下面五个概念，就能读懂 mongo-sync 的全部配置与日志。

---

## 1. 两条数据通路：全量 与 增量

| | 全量（Snapshot） | 增量（Change Stream / Oplog） |
|--|------------------|-------------------------------|
| 做什么 | 把源集合现有文档逐条读出写入 Sink | 持续消费源端变更日志，转成事件写入 Sink |
| 事件 `op` | `r`（snapshot） | `c` 插入 / `u` 更新 / `d` 删除 |
| 大表策略 | 按 `_id` 切段，多线程并行读 | 按写入顺序流式消费 |
| 失败重跑 | 表级切段进度（可选持久化） | 位点（ResumeToken / Oplog 时间戳） |

**关键设计：Sink 不感知上游是快照还是 CDC。** 两侧都变成统一事件后再写入，所以换捕获通道、换 Sink 都不影响下游。

---

## 2. 捕获通道：ChangeStream 与 Oplog

| | ChangeStream | Oplog |
|--|--------------|-------|
| 可读的源端 | 副本集、分片集群的 mongos | 仅单个副本集（含独立部署的 shard 副本集） |
| 不可读 | standalone 的增量 | standalone、mongos |
| MongoDB 版本 | 3.6+（分片增量需 3.6+） | 3.2 – 6.0；**≥ 7.0 上游已禁止** |
| 位点 | `resumeToken` / `startAtOperationTime` | oplog `ts`（`BsonTimestamp`） |
| 库级事件完整性 | 集合级 watch，多表时每表独立 watch，库级事件可能收不全 | 更完整 |

### `capture.mode` 三种取值

| 取值 | 行为 |
|------|------|
| `AUTO`（默认） | 按源端架构 + 版本 + `sync.mode` 自动匹配，选不出来就在启动时报错 |
| `CHANGE_STREAM` | 强制 ChangeStream；standalone 增量会失败 |
| `OPLOG` | 强制读 `local.oplog.rs`；mongos / standalone 上会失败，≥ 7.0 报错 |

`AUTO` 的匹配规则：

```text
全量（任何 sync.mode 含 FULL）
  └─ standalone / 副本集 / mongos 均可做集合扫描

增量（sync.mode 含增量）
  ├─ 副本集：MongoDB ≥ 3.6 → ChangeStream；< 3.6 → OPLOG
  ├─ 分片集群：ChangeStream@mongos（需 3.6+）  ← 多分片 OPLOG 已下线
  └─ standalone：不支持增量（只能用 sync.mode=FULL）
```

> 版本探测：`OPLOG` 模式可省略 `mongo.version`，启动时通过 `buildInfo` 自动探测。

---

## 3. 拓扑自适应

启动时通过 `hello` / `isMaster` 探测源端形态：

| 拓扑 | 判定依据 | 全量 | Oplog | ChangeStream |
|------|----------|------|-------|--------------|
| `STANDALONE` | 无 replica set 且非 mongos | ✅ | ❌ | ❌（仅 `FULL`） |
| `REPLICA_SET` | 副本集成员 | ✅ | ✅ | ✅ |
| `SHARDING` | 连接落在 mongos（`msg=isdbgrid`） | ✅ | ❌（改写为各 shard） | ✅ |

探测结果会打进启动日志，也会出现在 `progress` 的 `topology` 与 `captureMode`（AUTO 已展开）字段里：

```text
[mongo-sync] starting SYNC ns=demo.orders → demo.orders syncMode=FULL_AND_INCREMENTAL \
  capture=AUTO topology=REPLICA_SET resolvedCapture=CHANGE_STREAM
```

`capture.mode=AUTO` 时，"你配的什么" 和 "实际用什么" 是两件事，排障时看 `resolvedCapture`。

---

## 4. 有序写：分桶 + Disruptor 背压

CDC 场景下，同一文档的多次变更必须按顺序落地，否则最终状态会错。mongo-sync 的做法：

```text
MongoSourceClient
  → TransferEventListener / DdlEventListener
  → Caffeine ns CAS 锁
  → IdBucketRouter（hash(_id) % bucketNum）
  → 每桶一条 LMAX Disruptor RingBuffer（BlockingWaitStrategy 背压）
  → 同桶同 _id 再次出现，先 flush 前一条
  → MongoSinkClient / KafkaSinkClient
```

| 机制 | 作用 |
|------|------|
| `bucket.num` | 桶数量；`_id` 相同必然进同一桶 ⇒ 同文档串行 |
| `bucket.queue.capacity` | 每桶 RingBuffer 容量（Disruptor 要求 2 的幂，内部会向上取整），满则阻塞形成**背压**，不丢事件 |
| `force.single.bucket.on.unique.index` | 集合存在唯一索引（非 `_id`）时强制单桶，避免唯一约束被并发写破坏 |
| `ddl.wait.seconds` | 执行 DDL 前先等在途 CRUD 排空（DDL barrier） |
| `sink.writer.threads` | 不同命名空间可并发写 |

> 唯一索引在**启动时探测一次**，运行中新建的唯一索引不会改变分桶策略。

---

## 5. 位点与断点续传

| 捕获通道 | 位点类型 | 不配置 `offset.store.dir` 时 |
|----------|----------|------------------------------|
| ChangeStream | `resumeToken` | 仅内存，进程重启需重新开始 |
| Oplog | oplog `ts` | 仅内存，同上 |

配置 `offset.store.dir` 后，位点按命名空间落成文件，进程重启从上次位置继续。

增量从**全量开始之前**的位置开始消费（ChangeStream 用 `startAtOperationTime`），因此全量与增量之间必然存在一段重叠，重叠部分靠 UPSERT 兜底。

> ⚠️ 位点目前是**在 Source 回调后即保存**，而写入是异步的。崩溃时可能丢少量「已记位点但未落地」的事件。严格场景请配合 [数据校验](../guide/verify.md) 复核，或等待「写成功再记位点」的后续改进。

---

## 6. 事件模型

Sink 只认下面两类事件，与捕获通道无关：

| 模型 | 含义 |
|------|------|
| `TransferEvent` | 文档变更：`c` / `u` / `d` / `r` |
| `DdlEvent` | 删库 / 删表 / 建删索引 / 建表 / 改名 |

字段与 SPI 细节见 [mongo-transfer-model/README.md](../../mongo-transfer-model/README.md)。

---

## 7. Sink 形态

| `sink.type` | `sink.uri` | 行为 |
|-------------|------------|------|
| `mongodb`（默认） | MongoDB 连接串 | 按 `write.mode` / `on.conflict` 写入，可预建集合与索引、落地 DDL |
| `kafka` | Kafka bootstrap servers（可写 `kafka://` 前缀） | 把变更写成 **mongo-kafka 兼容的 Change Stream 消息**（JSON / BSON） |

Kafka 形态下不预建集合 / 索引（`bootstrap.collection` / `bootstrap.indexes` 被跳过并打日志），DDL 以消息形式投递（`kafka.publish.ddl`）。消息格式见 [mongo-kafka-sink-client/README.md](../../mongo-kafka-sink-client/README.md)。

---

## 8. 状态机与 cutover

```text
IDLE → RUNNING ⇄ CAN_COMMIT → COMMITTING → COMMITTED
         ↓
       PAUSED          STOPPED / ERROR
```

| 状态 | 含义 |
|------|------|
| `RUNNING` | 正在全量或增量 |
| `PAUSED` | 全量 + 增量都停（**初始全量进行中不允许**，请用 `pauseIncremental`） |
| `CAN_COMMIT` | 全量完成 + 管道排空 + 增量滞后达标 ⇒ 可以切流 |
| `COMMITTED` | 已停捕获、已排空写入，迁移结束 |

`canCommit` 的判定条件（缺一不可）：

1. 全量已完成（`fullSyncComplete`）
2. 管道在途事件为 0（`inflightEvents == 0`）
3. 增量滞后 ≤ `commit.max.lag.ms`（默认 10000 ms）
   —— 或增量流已空闲（无新事件持续 ≥ `maxLag` 且管道排空，适用于源端无写入）

进度字段含义见 [SDK 与控制面 · MigrationProgress](../guide/api.md#migrationprogress-字段)，各字段如何用于判断健康见 [运行与排障](../guide/operations.md#看懂-progress-日志)。

---

下一步：[快速开始](../getting-started/quickstart.md) · [同步模式](../guide/sync-modes.md)
