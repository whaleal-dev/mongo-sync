# SDK 与控制面

## 两个客户端

| 客户端 | 适用 | 配置类 |
|--------|------|--------|
| `MongoSyncClient` | 单表（源 ↔ 目标一对一） | `MongoSyncConfig` |
| `MongoMultiSyncClient` | 多库表 / 库级（白黑名单 + 命名空间变换） | `MongoMultiSyncConfig` |

```java
// 单表
MongoSyncClient sync = MongoSyncClient.create(MongoSyncClient.builder()
        .sourceUri("mongodb://src/?replicaSet=rs0")
        .sinkUri("mongodb://sink")
        .mapCollection("demo", "orders")
        .syncMode(SyncMode.FULL_AND_INCREMENTAL)
        .offsetStoreDir("./data/offsets")
        .writeErrorHandler((bucket, event, err) -> { /* 务必处理 */ }));

// 多表
MongoMultiSyncClient multi = MongoMultiSyncClient.create(MongoMultiSyncConfig.builder()
        .sourceUri("mongodb://src/?replicaSet=rs0")
        .sinkUri("mongodb://sink")
        .namespaceWhite("demo;app.orders")
        .syncMode(SyncMode.FULL_AND_INCREMENTAL)
        .offsetStoreDir("./data/offsets")
        .writeErrorHandler((bucket, event, err) -> { }));

sync.start();
multi.start();
```

> 也可以注入已存在的 `MongoClient`（`sourceMongoClient(...)` / `sinkMongoClient(...)`）以复用连接池；注入时该连接的生命周期由调用方负责，客户端不再关闭它。

### Builder 与配置键对照

配置文件的每个键都有等价的 Builder 方法，脚本试跑通过后可以直接翻译成代码：

| 配置键 | Builder 方法 |
|--------|--------------|
| `source.uri` / `sink.uri` | `sourceUri(...)` / `sinkUri(...)` |
| `source.database` + `source.collection` | `mapCollection(db, coll)`（同源同名）或 `sourceDatabase(...)` + `sourceCollection(...)` |
| `sink.database` / `sink.collection` | `sinkDatabase(...)` / `sinkCollection(...)` |
| `capture.mode` | `captureMode(CaptureMode.AUTO)` |
| `mongo.version` | `mongoVersion("4.4.29")` |
| `full.document` | `fullDocument(MongoSourceConfig.FullDocumentMode.UPDATE_LOOKUP)` |
| `enable.pre.image` | `enablePreImage(true)` |
| `include.from.migrate` | `includeFromMigrate(true)` |
| `sync.mode` | `syncMode(SyncMode.FULL_AND_INCREMENTAL)` |
| `write.mode` / `on.conflict` | `writeMode(WriteMode.UPSERT)` / `onConflict(OnConflict.SKIP)` |
| `sink.batch.size` / `sink.writer.threads` | `sinkBatchSize(1000)` / `sinkWriterThreads(8)` |
| `bucket.num` / `bucket.queue.capacity` | `bucketNum(16)` / `bucketQueueCapacity(8192)` |
| `ddl.wait.seconds` | `ddlWaitSeconds(30)` |
| `force.single.bucket.on.unique.index` | `forceSingleBucketOnUniqueIndex(false)` |
| `bootstrap.collection` / `bootstrap.indexes` / `skip.ttl.indexes` | 同名 Builder 方法 |
| `offset.log.interval.seconds` | `offsetLogIntervalSeconds(30)` |
| `offset.store.dir` | `offsetStoreDir("./data/offsets")` |
| `full.sync.*` | `fullSyncParallelism(...)` / `fullSyncBatchSize(...)` / `fullSyncTaskMbSize(...)` |
| `window.warn.seconds` | `windowWarnSeconds(3600)` |
| `commit.max.lag.ms` | `commitMaxLagMs(10_000L)` |
| `sink.type` / `kafka.*` | `sinkType(SinkType.KAFKA)` + `kafkaTopic...(...)` 系列 |

命名空间白黑名单与变换只在 `MongoMultiSyncConfig` 上：`namespaceWhite(...)` / `namespaceBlack(...)` / `namespaceTransform(...)`。

---

## 生命周期方法

| 方法 | 语义 | 典型用途 |
|------|------|----------|
| `start()` | 启动迁移；若处于 `PAUSED` 则从暂停处续跑（`resume()` 等价于 `start()`） | 启动 |
| `pause()` | 全量 + 增量都停，排空在途写入 | 临时冻结整个任务 |
| `pauseIncremental()` | **只冻增量**，全量扫描继续 | 全量期间要释放源端读压力 |
| `resumeIncremental()` | 恢复增量 | 与上一条配对 |
| `progress()` | 返回 `MigrationProgress` 快照 | 监控 / 判断能否切换 |
| `canCommit()` | 是否已到可提交状态 | 切换前的门禁 |
| `commit()` | 停捕获 + 排空写入 + 标记 `COMMITTED` | 最小 cutover |
| `isIncrementalPaused()` | 增量是否处于独立暂停 | 监控 |
| `inflightEvents()` | 在途事件数 | 判断是否真排空 |
| `getSourceTopology()` | 探测到的源端拓扑 | 排障 / 记录 |
| `getResolvedCaptureMode()` | `AUTO` 展开后的实际捕获通道 | 排障（最常用） |
| `stop()` / `close()` | 优雅停止并释放资源（幂等） | 关停 |

### 每次调用允许与否

某些操作在当前状态下会被**拒绝并抛出异常**，这是防止「暂停导致快照重放 / 重复写入」的刻意设计：

| 操作 | 被拒绝的情况 | 错误码 |
|------|--------------|--------|
| `start()` | 已 `stop()` | `MSYNC_STATE_001` |
| `start()` / `resume()` | 初始全量尚未完成就想 resume | `MSYNC_CTL_002` |
| `pause()` | 未 `start()` | `MSYNC_CTL_001` |
| `pause()` | **初始全量进行中**（请改用 `pauseIncremental()`） | `MSYNC_CTL_001` |
| `pause()` | 已 `COMMITTING` / `COMMITTED` | `MSYNC_CTL_001` |
| `pauseIncremental()` | 未 `start()`；已 `PAUSED` / `COMMITTING` / `COMMITTED`；`sync.mode` 不含增量 | `MSYNC_CTL_001` |
| `resumeIncremental()` | 整体已 `PAUSED`（应调用 `resume()`） | `MSYNC_CTL_002` |
| `commit()` | 未处于 `CAN_COMMIT` | `MSYNC_CTL_003`（异常里带 readiness 与 progress） |

> `commit()` 被拒绝时，异常信息里包含**没满足哪一条**，例如 `readiness=waiting inflight drain=1204`，据此定位即可。

---

## MigrationProgress 字段

`progress()` 返回的快照，也是 `progress` 日志与 HTTP `/api/v1/progress` 的内容。

| 字段 | 含义 |
|------|------|
| `namespace` | 命名空间（多表时为汇总） |
| `phase` | 相位：`NOT_STARTED` / `INITIAL_COPY` / `CHANGE_EVENT_APPLY` / `RUNNING` / `PAUSED_INITIAL_COPY` / `PAUSED_CHANGE_EVENT_APPLY` / `PAUSED` / `READY_TO_COMMIT` / `COMMITTING` / `COMMITTED` / `STOPPED` / `ERROR` |
| `topology` | 源端拓扑：`STANDALONE` / `REPLICA_SET` / `SHARDING` |
| `captureMode` | `AUTO` 展开后的实际捕获通道 |
| `syncMode` | 同步模式 |
| `state` | 迁移状态（`MigrationState` 枚举） |
| `canCommit` | 是否可提交 |
| `commitReadiness` | 不可提交时的原因（如 `waiting lagMs=... maxLagMs=...`） |
| `fullSyncComplete` | 全量是否已完成 |
| `estimatedTotalDocuments` | 源端文档数估算（`estimatedDocumentCount`） |
| `copiedDocuments` / `snapshotEvents` | 已拷贝文档数（`op=r` 事件数） |
| `remainingDocumentsEstimate` | 剩余文档估算 |
| `fullSyncPercent` | 全量百分比（无估算时：完成即 100） |
| `incrementalEvents` | 增量事件数 |
| `ddlEvents` | DDL 事件数 |
| `inflightEvents` | 在途事件数（管道未落地） |
| `lastEventTsMs` | 最近事件时间 |
| `lagMs` | 增量滞后（本机时间 − 最近事件时间） |
| `startedAtMs` / `committedAtMs` / `elapsedMs` | 启动 / 提交 / 已运行时长 |
| `namespaceCount` | 命名空间数量（多表时 > 1） |
| `incrementalPaused` | 增量是否独立暂停 |
| `windowRemainingSeconds` | 捕获窗口余量（秒）；无法探测为 `null` |
| `detail` | 状态补充说明 |

各字段的健康判读见 [运行与排障 · 看懂 progress 日志](operations.md#看懂-progress-日志)。

---

## 写失败回调

```java
.writeErrorHandler(new SyncWriteErrorHandler() {
    @Override
    public void onWriteError(int bucketId, TransferEvent event, Throwable error) {
        // bucketId：出错的桶；event：出错事件；error：异常
    }
})
```

| 要点 | 说明 |
|------|------|
| 不注入时 | 写失败**只打到 stderr**，**不会自动重试** |
| 建议 | 落盘 / 告警 / 计数，并纳入监控；`bucketId` 可用于定位是否集中在某几个桶 |
| 与 `on.conflict` 的关系 | `on.conflict=SKIP` 处理的冲突**不算**写失败；这里的回调是真正写入失败 |

---

## 位点 SPI

不满足于「文件或内存」时，可实现 SPI 自行接管位点：

| 通道 | SPI | 说明 |
|------|-----|------|
| ChangeStream | `ResumeTokenStorage` | 保存 / 读取 `resumeToken` |
| Oplog | `OplogOffsetStorage` | 保存 / 读取 oplog `ts` |

通过 Builder 的 `resumeTokenStorage(...)` / `oplogOffsetStorage(...)` 注入。接口定义与示例见 [mongo-source-client/README.md](../../mongo-source-client/README.md#spi-扩展)。

---

## HTTP 控制面

配置文件启动（`SyncMain`）时，置 `http.enabled=true` 可启用：

```properties
http.enabled=true
http.host=127.0.0.1
http.port=27182
```

| 端点 | 方法 | 作用 |
|------|------|------|
| `/api/v1/progress` | GET | 返回完整 `MigrationProgress` |
| `/api/v1/canCommit` | GET | 返回 `canCommit` / `state` / `commitReadiness` / `incrementalPaused` / `windowRemainingSeconds` |
| `/api/v1/pause` | POST | 整体暂停 |
| `/api/v1/resume` | POST | 恢复 |
| `/api/v1/pauseIncremental` | POST | 只冻增量 |
| `/api/v1/resumeIncremental` | POST | 恢复增量 |
| `/api/v1/commit` | POST | 提交（不满足条件返回 500 + readiness） |

```bash
curl -s http://127.0.0.1:27182/api/v1/progress | jq '.progress.phase, .progress.lagMs'
curl -s http://127.0.0.1:27182/api/v1/canCommit | jq
curl -s -X POST http://127.0.0.1:27182/api/v1/commit | jq
```

响应统一为 `{"success": true|false, ...}`；失败时带 `error` 与 `type`。方法不匹配返回 `405`。

> ⚠️ 接口**没有鉴权**，默认只监听 `127.0.0.1`。要对外暴露请自行加访问控制。

---

## 错误码

启动与运行控制的错误码（`MongoSyncErrorCode`），异常信息形如 `[mongo-sync] ERROR code=MSYNC_CFG_002 message=...`：

| 错误码 | 枚举 | 含义 |
|--------|------|------|
| `MSYNC_CFG_001` | `CONFIG_REQUIRED` | 缺少必填配置 |
| `MSYNC_CFG_002` | `CONFIG_INVALID` | 配置值非法 / 含已移除键 |
| `MSYNC_ARG_001` | `ARGUMENT_UNKNOWN` | 未知命令行参数 |
| `MSYNC_FILE_001` | `FILE_NOT_FOUND` | 配置文件不存在 |
| `MSYNC_STATE_001` | `CLIENT_STATE_INVALID` | 客户端状态不允许该操作（如已停止） |
| `MSYNC_CTL_001` | `PAUSE_NOT_ALLOWED` | 当前状态/模式不允许暂停 |
| `MSYNC_CTL_002` | `RESUME_NOT_ALLOWED` | 当前状态不允许恢复 |
| `MSYNC_CTL_003` | `COMMIT_NOT_ALLOWED` | 未达 `CAN_COMMIT` 不能提交 |
| `MSYNC_CTL_004` | `MULTI_OPERATION_PARTIAL_FAILURE` | 多表 pause / commit 部分子任务失败（已尽力处理全部子任务） |

脚本自身还有一组退出码：`2`（参数错）/ `20`（已有实例在跑）/ `21`（pid 文件不存在）/ `22`（失效 pid）。见 [快速开始 · 脚本的进程管理](../getting-started/quickstart.md#脚本的进程管理)。

---

下一步：[数据校验](verify.md) · [运行与排障](operations.md)
