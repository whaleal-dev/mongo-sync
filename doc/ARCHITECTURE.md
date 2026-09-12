# 架构与功能审查（Java 8+）

> 审查范围：`mongo-sync` 全模块。目标运行时：**Java 8+**。  
> 关系型同步（MySQL / Oracle / PostgreSQL）见姊妹仓 [rds-sync](https://github.com/whaleal-dev/rds-sync)；两边编排语义对齐，事件模型独立。

## 1. 模块架构

```text
mongo-sync/
├── mongo-transfer-model/   通用 TransferEvent / DdlEvent / TransferSink（捕获无关）
├── mongo-source-client/    ChangeStream / Oplog → 传输模型
├── mongo-sink-client/      只识别传输模型 → MongoDB 写入 / DDL
├── mongo-kafka-sink-client 只识别传输模型 → Kafka（mongo-kafka Change Stream 格式）
├── mongo-sync-client/      Source → Disruptor 分桶 + Caffeine 锁 → TransferSink
└── doc/oplog/              各版本 oplog 样例（自 d2t 拷贝）
```

数据契约：

| 模型 | 含义 |
|------|------|
| `TransferEvent` | 文档变更 c/u/d/r |
| `DdlEvent` | 删库/删表/建删索引/建表/改名 |

Sink **不感知** Oplog 还是 ChangeStream。

与 [rds-sync](https://github.com/whaleal-dev/rds-sync) 的关系：本仓事件是文档 `TransferEvent`；rds-sync 是行级 `RowChange`。两边都可以把 Kafka 当 Sink：本仓是 mongo-kafka Change Stream，rds-sync 是行级 envelope。控制 API 与 SyncMode 同构，但**不要**把两个 SDK 串成一条异构链路（没有官方 Mongo ↔ MySQL 适配器）。

## 2. 同步链路（mongo-sync-client）

```text
MongoSourceClient
  → TransferEventListener / DdlEventListener
  → Caffeine ns CAS 锁
  → IdBucketRouter（_id % bucketNum；唯一索引强制单桶）
  → 每桶 LMAX Disruptor RingBuffer（BlockingWaitStrategy 背压）
  → 同桶同 _id 再次出现先 flush
  → MongoSinkClient / KafkaSinkClient
```

### SyncMode（全量 / 增量）

| 模式 | 行为 |
|------|------|
| `FULL` | 仅全量 |
| `FULL_AND_INCREMENTAL` | **并行**：全量与增量同时跑，全量结束后持续增量 |
| `FULL_AND_CATCH_UP` | **并行**：同上；全量结束后设上界，追平后停止 |
| 命名说明 | 用 `AND` 表示并行，不用易误解为串行的 `THEN` |
| 删表/改名提前结束全量 | 增量识别 `DROP` / `RENAME` / `DROP_DATABASE` → 源端全量视为完成 |
| 索引 DDL 刷新分桶 | `CREATE/DROP_INDEXES` 后重探唯一索引并调整 Sink ordered |
| rename 后 Sink 跟随 | Sink 执行 rename 后切换写集合句柄 |
| `INCREMENTAL` | 仅增量 |

衔接要点：增量从全量**开始前**的 oplog ts / `startAtOperationTime` 消费；与快照重复靠 UPSERT；全量结束后 `tryDrainAndFlush`（并行下不强求 inflight=0）。

**不采用** Reactor / Reactive 重写内核。

## 3. 已具备能力

| 能力 | 状态 |
|------|------|
| ChangeStream CRUD + DDL 回调 | ✅ |
| Oplog 3.2–6.0（V1/V2/V3）+ DDL | ✅ |
| SyncMode 四模式（含全量∥增量） | ✅ |
| 源端架构自动匹配（standalone / replicaSet / sharding → 读任务） | ✅ |
| ChangeStream `startAtOperationTime` 衔接 | ✅ |
| Sink 文档 UPSERT / `$set/$unset` / `onConflict` | ✅ |
| Sink DDL 落地 | ✅ |
| 分桶有序 + Disruptor | ✅ |
| Caffeine ns 锁 / 唯一索引（启动探测一次） | ✅ |
| 启动前表结构预建（`bootstrapCollection` / `bootstrapIndexes` 可分别开关） | ✅ |
| 多库表白/黑名单 + ns 变换（`MongoMultiSyncClient`） | ✅ |
| 分片增量 ChangeStream@mongos（≥3.6；多分片 OPLOG 已移除） | ✅ |
| `canCommit` + `commit.max.lag.ms` | ✅ |
| 捕获窗口告警（`window.warn.seconds`） | ✅ |
| 独立增量 pause（`pauseIncremental`） | ✅ |
| 数据比对校验（`VerifyMain`：COUNT/ID/FULL） | ✅ |
| Kafka Sink（mongo-kafka Change Stream JSON/BSON） | ✅ |
| 位点文件持久化（`offsetStoreDir`，按 ns） | ✅ |
| 位点周期心跳日志 | ✅ |
| 大表全量并行读（`fullSyncParallelism`，对齐 d2t `_id` 切段） | ✅ |
| Java 8 编译 | ✅（Caffeine 2.9.3 / Disruptor 3.4.4） |

## 4. 架构 / 功能漏洞与限制

| 级别 | 问题 | 说明 / 建议 |
|------|------|-------------|
| 中 | 写失败默认仅 stderr | 已提供 `SyncWriteErrorHandler`；未注入时仍只打 stderr，**不自动重试** |
| 中 | 位点在 Source 回调后即保存 | Sync 异步写入时，崩溃可能丢未落库事件；严格场景需「写成功再记位点」 |
| 中 | ChangeStream 集合级 watch | 多表时每表独立 watch；`dropDatabase` 等库级事件可能收不全；Oplog 更完整 |
| 中 | 分片 orphan / balancer | 分片增量走 ChangeStream@mongos；未做 orphan 文档过滤；`includeFromMigrate` 可控 fromMigrate |
| 低 | ≥7.0 禁 Oplog | 已构建期校验，须 ChangeStream |
| 低 | 同 ns 跨批异步写 | 默认 8 写线程；同 `_id` 靠分桶内 `flushAndWait`；不同 ns 可并发 |
| — | ~~单集合同步~~ | 已补：`MongoMultiSyncClient` 白名单多表 |
| — | ~~位点不做持久化~~ | 已补：可选 `offsetStoreDir` 文件位点 |
| — | ~~DDL barrier TOCTOU~~ | 已修：`offer` 在 `inflight++` 后复检 barrier |
| — | ~~null 事件 inflight 泄漏~~ | 已修：Handler 始终 `finally` 递减 |
| — | ~~ns 锁 Caffeine 过期~~ | 已改为 `ConcurrentHashMap` 不过期 |
| — | ~~唯一索引缓存过期~~ | 已去掉 expire，避免运行中改分桶策略 |
| — | ~~flushAndWait 漏等 Future~~ | 已修：循环排空 inflight 列表 |
| — | ~~ns CAS 掐死全量∥增量~~ | Sync 已去掉 offer 路径 ns 锁，Disruptor MULTI 真并行 |

## 5. 依赖与 Java 8

| 组件 | Java 8 版本 |
|------|-------------|
| JDK | 1.8+ |
| mongodb-driver-sync | 4.11.1 |
| kafka-clients | **3.6.2**（Java 8；仅 Kafka Sink） |
| caffeine | **2.9.3**（勿用 3.x） |
| disruptor | **3.4.4**（勿用 4.x） |

```bash
mvn clean install -DskipTests
```

## 6. 文档索引

| 文档 | 内容 |
|------|------|
| [README.md](../README.md) | 工程总览 |
| [mongo-transfer-model/README.md](../mongo-transfer-model/README.md) | 传输模型 |
| [mongo-source-client/README.md](../mongo-source-client/README.md) | Source API |
| [mongo-sink-client/README.md](../mongo-sink-client/README.md) | Sink API（MongoDB） |
| [mongo-kafka-sink-client/README.md](../mongo-kafka-sink-client/README.md) | Kafka Sink / Change Stream 消息 |
| [mongo-sync-client/README.md](../mongo-sync-client/README.md) | 同步编排 / SyncMode |
| [doc/examples/](../doc/examples/) | 配置文件示例（properties） |
| [doc/oplog/](../doc/oplog/) | Oplog 样例 |
| [rds-sync 架构](https://github.com/whaleal-dev/rds-sync/blob/main/docs/ARCHITECTURE.md) | 关系库同步契约（控制面对齐本仓） |

## 7. 建议后续（按优先级）

1. 分片 orphan 过滤 / balancer 协同（对齐 MongoShake）  
2. 写成功后再推进位点  
3. 共享 Oplog 单通道多 ns（降低每表一条 ChangeStream 的开销）  
4. 独立进程启动器 + 监控 API（可选）
