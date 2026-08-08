# mongo-sync-client

文档数据库同步编排模块（参考 **d2t** + **LMAX Disruptor**）：

```text
Source (Oplog / ChangeStream)
    ↓  TransferEvent / DdlEvent
分桶 (_id hash) → 每桶 Disruptor RingBuffer
    + Caffeine ns/DDL 锁、唯一索引缓存
    ↓
Sink 落地
```

Sink **只认** `TransferEvent` / `DdlEvent`，不感知捕获协议。

## 对齐 d2t 的设计点

| d2t | 本模块 |
|-----|--------|
| `_id.hashCode % bucketNum` | `IdBucketRouter` |
| 唯一索引强制 bucket=1 | Caffeine `uniqueIndexCache` → bucket=0；Sink `ordered=true` |
| 同桶同 `_id` 先 flush | 桶线程 `pendingIds`（`_id`→写入序号）；`landedThrough` 裁剪，有界；重复 `_id` 且前次未落库时 `flushAndWait` |
| 表级 CAS 锁 `stateOfNsMap` | Caffeine `nsParseLocks` |
| DDL 前等待在途 | `ddlBarrier` + `ddlWaitSeconds` 排空；DDL 后刷新唯一索引缓存 / Sink ordered；rename 后 Sink retarget |
| WriteModel 分桶写 | **每桶 Disruptor**（RingBuffer + 单 Handler）→ `sink.write` |
| 有界背压 | `BlockingWaitStrategy`：Ring 满时 `next()` 阻塞生产者 |

## 命名约定

| 层级 | 用词 | 含义 |
|------|------|------|
| Sync 配置 | `source*` / `target*` | 源端 / 目标端连接与库表、批量参数 |
| 模块 / 组件 | `mongo-sink-client` / `MongoSinkClient` | 写入落地 SDK（实现角色） |

例如：`.targetUri()` / `.targetBatchSize()`；内部持有 `MongoSinkClient` 实例。

## 同步模式（`SyncMode`）

| 枚举 | 行为 |
|------|------|
| `FULL` | 仅全量，结束后停止 |
| `FULL_AND_INCREMENTAL` | **全量∥增量**：先记位点，增量与快照并行；全量结束后持续增量 |
| `FULL_AND_CATCH_UP` | **全量∥增量**：同上；全量结束后设上界，追平后停止 |
| `INCREMENTAL` | 仅增量（可用 `oplogStartTimestamp` / `oplogEndTimestamp` 限窗） |

```java
.syncMode(SyncMode.FULL_AND_INCREMENTAL)
```

## 全量 ∥ 增量衔接

```text
beforeInitialSync：记录 start（oplog ts / clusterTime），清旧 ResumeToken
        │
        ├───────────── 增量线程：从 start 之后持续消费 ──────────►
        │
        └───────────── 全量线程：扫描集合 op=r ──────────────►
                              │
                              ▼
                    onInitialSyncCompleted
                    （CATCH_UP：写入 end 上界）
                              │
                              ▼
                    tryDrainAndFlush（尽力排空，不因并行增量超时失败）
```

- 快照窗口内变更由并行增量覆盖；与 `op=r` 重复靠 **UPSERT**  
- 并行期间**禁止回拨位点**（避免覆盖增量已推进的 offset/token）  
- `FULL_AND_CATCH_UP`：上界在全量结束后写入；增量动态感知上界并追平后停  
- 增量识别到本表 `DROP_COLLECTION` / `RENAME_COLLECTION` / 本库 `DROP_DATABASE`：源端全量扫描**视为完成**并退出，仍走 `onInitialSyncCompleted` / barrier  
- 过程中元信息：`CREATE/DROP_INDEXES` 后重探唯一索引并调整分桶/ordered；`RENAME` 后目标写集合跟随改名（源监视名不自动跟随，ChangeStream 通常随后 invalidate）  

## 并发注意

- 全量∥增量：双线程 `onEvent` → Disruptor `MULTI`；DDL 用 `ddlBarrier`（offer 双检）
- Sink 多写线程：`flushAndWait` 会等到提交后的全部 Future；同桶同 `_id` 先 flush
- `tryDrainAndFlush`：并行增量下不强求 `inflight==0`

## 启动前表结构预建

`start()` 时按配置预建元数据（均可关闭）：

| 配置 | 默认 | 含义 |
|------|------|------|
| `.bootstrapCollection(true)` | true | 创建目标集合/视图 |
| `.bootstrapIndexes(true)` | true | 创建源端非 `_id_` 索引 |
| `.skipTtlIndexes(true)` | true | 预建索引时跳过 TTL（仅 `bootstrapIndexes=true` 时生效） |

目标集合已存在则跳过建表；`bootstrapIndexes=true` 时仍会补齐缺失索引。

## 数据比对（校验）

入口类：`com.whaleal.third.mongo.sync.verify.VerifyMain`

```bash
# 使用配置文件
mvn -pl mongo-sync-client -am package -DskipTests
mvn -pl mongo-sync-client exec:java \
  -Dexec.args="../doc/examples/mongo-verify.example.properties"

# 或命令行单表
mvn -pl mongo-sync-client exec:java -Dexec.args="\
  --source-uri mongodb://127.0.0.1:27017/?replicaSet=rs0 \
  --target-uri mongodb://127.0.0.1:27018 \
  --source-db demo --source-coll orders --mode FULL"
```

| `verify.mode` | 含义 |
|---------------|------|
| `COUNT` | 仅文档数 |
| `ID` | 比对 `_id` 有无（缺源/缺目标） |
| `FULL` | `_id` + 文档内容（可用 `verify.ignore.fields` 忽略字段） |

退出码：`0` 通过，`1` 有差异，`2` 运行错误。示例配置见 [mongo-verify.example.properties](../doc/examples/mongo-verify.example.properties)。

## 多库表同步（补齐 MongoShake 级过滤）

```java
MongoMultiSyncClient multi = MongoMultiSyncClient.create(MongoMultiSyncConfig.builder()
        .sourceUri("mongodb://src/?replicaSet=rs0")
        .targetUri("mongodb://target")
        .namespaceWhite("demo;app.orders")   // 整库 demo + 单表 app.orders
        // .namespaceTransform("demo.orders:backup.orders")
        .syncMode(SyncMode.FULL_AND_INCREMENTAL)
        .offsetStoreDir("./data/offsets")    // 按 ns 文件持久化位点
        .writeErrorHandler((bucket, event, err) -> { /* ... */ }));
multi.start();
```

| API | 能力 |
|-----|------|
| `namespaceWhite` / `namespaceBlack` | 库或 `db.coll`，分号分隔，互斥 |
| `namespaceTransform` | `src.ns:tgt.ns` 映射 |
| `offsetStoreDir` | 文件位点（单表 `MongoSyncClient` 同样支持） |
| `sourceOplogUris` | 分片 OPLOG 多源（每 shard 一条）；**可不配**，`captureMode=OPLOG` 时自动 `listShards` |
| `captureMode` | 默认 `AUTO`：按源端架构匹配读任务 |

### 源端架构自动匹配读任务

启动时 `hello`/`isMaster` 识别三种架构，并匹配增量读方式：

| 架构 | 全量 | 增量 OPLOG | 增量 ChangeStream |
|------|------|------------|-------------------|
| **standalone** | ✅ | ❌ | ❌（仅 `syncMode=FULL`） |
| **replicaSet** | ✅ | ✅（直连 mongod/RS） | ✅ |
| **sharding** | ✅（mongos） | ✅（各 **shard**，禁 mongos） | ✅（mongos） |

`captureMode=AUTO` 默认：副本集 → ChangeStream；分片 → ChangeStream@mongos（mongos 已做跨分片归并与全局定序）；仅当服务端 &lt; 3.6 不支持 ChangeStream 时才回退到全量@mongos + 各 shard OPLOG。

> 多分片 OPLOG 由各 shard 独立读取，**没有全局序**：同一条 DDL 会在每个 shard 各出现一次，且可能与其他 shard 的 CRUD 乱序（如先删表后插入）。需要该模式请显式配置 `captureMode=OPLOG`。

```java
// 推荐：不手配 capture / shard URI，连 mongos 即可
MongoSyncClient sync = MongoSyncClient.create(MongoSyncClient.builder()
        .sourceUri("mongodb://mongos:27017")
        .targetUri("mongodb://target:27017")
        .mapCollection("demo", "orders")
        .syncMode(SyncMode.FULL_AND_INCREMENTAL)
        .captureMode(CaptureMode.AUTO)   // 默认即可省略
        .offsetStoreDir("./data/offsets")
        .writeErrorHandler((b, e, err) -> {}));
System.err.println(sync.getSourceTopology() + " → " + sync.getResolvedCaptureMode());
```

### 分片 OPLOG（多源，可手动覆盖）

```java
MongoSyncClient sync = MongoSyncClient.create(MongoSyncClient.builder()
        .sourceUri("mongodb://mongos:27017")           // 全量 / 元数据
        .sourceOplogUris(
                "mongodb://s0:27017/?replicaSet=rs0",
                "mongodb://s1:27017/?replicaSet=rs1")
        .sourceOplogShardNames("shard0", "shard1")
        .captureMode(CaptureMode.OPLOG)
        // .mongoVersion("4.4.29")  // 可省略，buildInfo 自动探测
        .mapCollection("demo", "orders")
        .syncMode(SyncMode.FULL_AND_INCREMENTAL)
        .offsetStoreDir("./data/offsets")
        .writeErrorHandler((b, e, err) -> {}));
```

单表仍用 `MongoSyncClient` + `mapCollection` / `sourceDatabase`+`sourceCollection`。

## 快速开始

```java
MongoSyncClient sync = MongoSyncClient.create(MongoSyncClient.builder()
        .sourceUri("mongodb://127.0.0.1:27017/?replicaSet=rs0")
        .targetUri("mongodb://127.0.0.1:27018")
        .sourceDatabase("demo").sourceCollection("orders")
        .targetDatabase("demo").targetCollection("orders")
        // captureMode 默认 AUTO → 副本集走 ChangeStream
        .syncMode(SyncMode.FULL_AND_INCREMENTAL)
        .bootstrapCollection(true)  // 默认：先建表
        .bootstrapIndexes(true)     // 默认：再建索引；仅同步数据时可设 false
        .bucketNum(16)
        .ddlWaitSeconds(30)
        .writeErrorHandler((bucket, event, err) -> {
            // 生产务必处理写失败，避免静默丢事件
        }));

sync.start();
// ...
sync.close();
```

Oplog 模式：

```java
MongoSyncClient.create(MongoSyncClient.builder()
        .captureMode(CaptureMode.OPLOG)
        .mongoVersion("4.4.29")
        // ...
);
```

## 生产消费模型

- **分桶通道**：LMAX Disruptor（每桶独立 RingBuffer，容量为 2 的幂，默认 8192）
- **锁/元数据缓存**：Caffeine
- **不再使用** `BlockingQueue` 做分桶传递
- **位点心跳**：默认每 30s 打印当前位点与上次同步时间（`.offsetLogIntervalSeconds(0)` 关闭）；重试/停机也会打印
- **全量并行读**（对齐 d2t）：`.fullSyncParallelism(n)`（>1 时按 `_id` 切段并行）；`.fullSyncTaskMbSize(32)` 控制单段体积；`.fullSyncBatchSize` 控制游标 batch
- **位点存储**：可选 `.offsetStoreDir(...)` 文件持久化；未配置则为进程内内存

## Maven

```xml
<dependency>
  <groupId>com.whaleal.third</groupId>
  <artifactId>mongo-sync-client</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

配置项示例见 [doc/examples/mongo-sync.example.properties](../doc/examples/mongo-sync.example.properties)。

## 能力边界

- ✅ **Java 8+**（Caffeine 2.9.3 / Disruptor 3.4.4）
- ✅ 单集合：`SyncMode` 四模式（全量∥增量并行）+ DDL
- ✅ 多集合/库级：`MongoMultiSyncClient` + 白/黑名单 + ns 变换
- ✅ 位点可选文件持久化：`offsetStoreDir`
- ✅ 分桶有序、幂等 UPSERT；默认 **8** 写线程（不同 ns 可并发）
- ✅ `SyncWriteErrorHandler` 写失败回调
- ❌ 暂不支持分片 orphan/balancer 专项（可用 `includeFromMigrate`）
- ❌ 位点仅内存、不做持久化（当前阶段明确不做；靠周期日志看上次同步时间）
- ❌ 默认不保证写成功后再推进位点（异步写入场景）

详见 [架构说明](../doc/ARCHITECTURE.md)。
