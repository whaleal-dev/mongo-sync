# 配置项全解

配置来源是 `java.util.Properties` 文件（脚本 / `--config` 传入），或等价的 Builder 调用。键名在两侧完全一致，便于「先脚本试跑、再改成代码」。

- 配置文件按 **UTF-8** 读取
- 命令行参数（`--sync-mode` 等）会**覆盖**配置文件中的同名键
- Builder 调用等价于逐键显式设置

---

## 最小可用配置

**单表（MongoDB Sink）**

```properties
source.uri=mongodb://127.0.0.1:27017/?replicaSet=rs0
sink.uri=mongodb://127.0.0.1:27018
source.database=demo
source.collection=orders
sync.mode=FULL_AND_INCREMENTAL
offset.store.dir=./data/mongo-sync-offsets
```

**多表 / 库级（MongoDB Sink）**

```properties
source.uri=mongodb://127.0.0.1:27017/?replicaSet=rs0
sink.uri=mongodb://127.0.0.1:27018
namespace.white=demo;app.orders
sync.mode=FULL_AND_INCREMENTAL
offset.store.dir=./data/mongo-sync-offsets
```

**Kafka Sink**

```properties
source.uri=mongodb://127.0.0.1:27017/?replicaSet=rs0
sink.type=kafka
sink.uri=127.0.0.1:9092
source.database=demo
source.collection=orders
kafka.topic.prefix=mongo
sync.mode=FULL_AND_INCREMENTAL
offset.store.dir=./data/mongo-sync-offsets
```

完整可复制模板见 [examples/](../examples/)：`mongo-sync.example.properties`、`mongo-sync-kafka.example.properties`。

---

## 必填与互斥规则

| 规则 | 说明 |
|------|------|
| `source.uri` / `sink.uri` | **必填** |
| 单表模式 | 必须给 `source.database` + `source.collection` |
| 多表模式 | 给了 `namespace.white` 或 `namespace.black` 即为多表模式，此时**禁止**再给 `source.database` / `source.collection`，否则启动失败 |
| 白 / 黑名单 | `namespace.white` 与 `namespace.black` **互斥**，同时非空直接失败 |
| `sink.type=kafka` | `sink.uri` 视为 bootstrap servers，`bootstrap.table` 类预建行为被强制关闭 |

启动时的错误码见 [SDK 与控制面 · 错误码](api.md#错误码)。

---

## 一、连接

| 键 | 默认 | 说明 |
|----|------|------|
| `source.uri` | — | 源 MongoDB 连接串。**必填** |
| `sink.type` | `mongodb` | Sink 形态：`mongodb` \| `kafka` |
| `sink.uri` | — | `mongodb`：目标连接串；`kafka`：bootstrap servers（可带 `kafka://` 前缀）。**必填** |

## 二、命名空间（单表模式）

| 键 | 默认 | 说明 |
|----|------|------|
| `source.database` | — | 源库名 |
| `source.collection` | — | 源集合名 |
| `sink.database` | 同源 | 目标库名 |
| `sink.collection` | 同源 | 目标集合名 |

## 三、命名空间（多表 / 库级模式）

| 键 | 默认 | 说明 |
|----|------|------|
| `namespace.white` | — | 白名单，分号分隔。条目可为整库 `db` 或单集合 `db.collection` |
| `namespace.black` | — | 黑名单，分号分隔，格式同上。与白名单互斥 |
| `namespace.transform` | — | 命名空间变换，分号分隔，形如 `srcDb.srcColl:tgtDb.tgtColl`；未命中则同源同名 |

系统库（`admin` / `local` / `config`）与 `system.*` 集合**始终跳过**。

```properties
namespace.white=demo;app.orders;app.users
namespace.transform=demo.orders:backup.orders_copy
```

## 四、捕获

| 键 | 默认 | 说明 |
|----|------|------|
| `capture.mode` | `AUTO` | `AUTO` \| `CHANGE_STREAM` \| `OPLOG`，见 [概念总览 · 捕获通道](../concepts/overview.md#2-捕获通道changestream-与-oplog) |
| `mongo.version` | 自动探测 | 显式指定源端 MongoDB 版本（如 `4.4.29`）。`OPLOG` 可省略；不填时启动通过 `buildInfo` 探测 |
| `full.document` | `DEFAULT` | ChangeStream `fullDocument`：`DEFAULT` \| `UPDATE_LOOKUP` \| `WHEN_AVAILABLE` \| `REQUIRED`。需要 update 事件带完整文档时用 `UPDATE_LOOKUP` |
| `enable.pre.image` | `false` | 是否请求 pre-image（副本集需开启相应能力） |
| `include.from.migrate` | `false` | 是否包含 `fromMigrate` 事件；分片搬迁期如需连同迁移事件一起处理再打开 |

> 旧键 `source.oplog.uris`、`source.oplog.shard.names` **已被移除**（多分片 OPLOG 下线）。带着这两个键启动会**直接失败并提示**，而不是被静默忽略。

## 五、同步模式

| 键 | 默认 | 说明 |
|----|------|------|
| `sync.mode` | 配置文件启动：`FULL_AND_INCREMENTAL`<br>SDK 不设置：`INCREMENTAL` | `FULL` \| `FULL_AND_INCREMENTAL` \| `FULL_THEN_CATCH_UP` \| `INCREMENTAL` |

> ⚠️ **默认值不一致，是最容易踩的坑**：用配置文件跑默认就是「全量∥增量」；用 SDK 忘了 `.syncMode(...)`，默认只有增量，**不会做全量**。SDK 场景请显式设置。

模式语义详见 [同步模式](sync-modes.md)。

## 六、元数据预建

| 键 | 默认 | 说明 |
|----|------|------|
| `bootstrap.collection` | `true`（`sink.type=kafka` 时 `false`） | 启动前从源端读集合定义，在 Sink 端创建集合 / 视图 |
| `bootstrap.indexes` | `true`（`sink.type=kafka` 时 `false`） | 启动前在 Sink 端创建源端非 `_id` 索引。需 Sink 集合已存在，或同时开启 `bootstrap.collection` |
| `skip.ttl.indexes` | `true` | 建索引时跳过 TTL（`expireAfterSeconds`）索引 |

> `sink.type=kafka` 时无论怎么配都会被跳过，并打印 `skip bootstrapCollection/indexes: sinkType=KAFKA`。

## 七、写入

| 键 | 默认 | 说明 |
|----|------|------|
| `write.mode` | `UPSERT` | `UPSERT`：insert/update/replace 走 upsert，适合 CDC 幂等落地；`STRICT`：按 `op` 严格执行，insert 为 `InsertOne` |
| `on.conflict` | `FAIL` | 唯一键冲突（duplicate key 11000）策略：`FAIL` 抛异常 \| `SKIP` 跳过并记日志 \| `UPSERT` 把冲突的 insert 转为按 `_id` 的 upsert 重试 |
| `sink.batch.size` | `1000` | Sink 端 bulk 批量大小。**必须 > 0** |
| `sink.writer.threads` | `8` | Sink 写线程数。**必须 > 0** |

`write.mode` 与 `on.conflict` 的配合：

| 场景 | 建议 |
|------|------|
| 全量∥增量（默认） | `write.mode=UPSERT`，重叠窗口靠 `_id` 幂等覆盖 |
| 有唯一二级索引，可能出现冲突 | `on.conflict=SKIP`（跳过并记日志）或 `UPSERT` |
| 要求严格按变更序列执行、不接受静默修正 | `write.mode=STRICT` + `on.conflict=FAIL`，由 `SyncWriteErrorHandler` 兜住 |

## 八、分桶与有序写

| 键 | 默认 | 说明 |
|----|------|------|
| `bucket.num` | `16` | 分桶数。**必须 > 0**；同 `_id` 必进同桶 ⇒ 同文档串行 |
| `bucket.queue.capacity` | `8192` | 每桶 RingBuffer 容量，Disruptor 要求 2 的幂（内部向上取整） |
| `ddl.wait.seconds` | `30` | 执行 DDL 前等待在途 CRUD 排空的超时秒数 |
| `force.single.bucket.on.unique.index` | `true` | 集合存在非 `_id` 唯一索引时强制单桶，避免唯一约束被并发写破坏 |

## 九、位点

| 键 | 默认 | 说明 |
|----|------|------|
| `offset.store.dir` | 空（仅内存） | 位点文件持久化目录，按命名空间一份文件。**生产强烈建议配置**，否则进程重启不续传 |
| `offset.log.interval.seconds` | `30` | 周期打印当前位点（oplog `ts` / `clusterTime`）的间隔秒数；`<=0` 关闭 |

## 十、进度与提交

| 键 | 默认 | 说明 |
|----|------|------|
| `progress.log.interval.seconds` | `10` | 打印 `MigrationProgress` 的间隔秒数；`<=0` 关闭。**必须 >= 0**（负数启动失败） |
| `commit.when.ready` | `false` | 达到 `canCommit` 后是否自动执行 `commit` 并退出进程 |
| `commit.max.lag.ms` | `10000` | 允许提交的最大增量滞后（毫秒），参与 `canCommit` 判定 |
| `window.warn.seconds` | `3600` | 捕获窗口告警阈值（秒）：锚定位点相对 oplog 最早条目的余量。逼近时打 `WINDOW WARN`；`<=0` 关闭 |

`window.warn.seconds` 只在全量∥增量期间有意义：全量扫描可能耗时很长，而源端 oplog 容量固定，位点一旦被「追上并覆盖」就再也追不回来了。这个告警就是提前告诉你「该加快全量或扩 oplog 了」。

## 十一、全量并行读

| 键 | 默认 | 说明 |
|----|------|------|
| `full.sync.parallelism` | `1` | 全量并行读线程数。`1` = 单游标；`>1` 按 `_id` 切段并行扫描 |
| `full.sync.batch.size` | `1000` | 全量游标 `batchSize` |
| `full.sync.task.mb.size` | `32` | 单段任务目标体积（MB），用于估算 skip 切段大小 |

## 十二、Kafka Sink

`sink.type=kafka` 时生效。`sink.uri` 为 bootstrap servers。

| 键 | 默认 | 说明 |
|----|------|------|
| `kafka.topic` | 空 | 固定 topic。非空时**覆盖**按命名空间拼接 |
| `kafka.topic.prefix` | 空 | topic 前缀 |
| `kafka.topic.separator` | `.` | 分隔符 |
| `kafka.topic.suffix` | 空 | topic 后缀 |
| `kafka.output.format` | `JSON` | value 编码：`JSON`（Extended/STRICT JSON，兼容 mongo-kafka）\| `BSON` |
| `kafka.publish.ddl` | `true` | 是否把 DDL 作为消息投递 |
| `kafka.acks` | `all` | Producer `acks` |
| `kafka.linger.ms` | `5` | Producer `linger.ms` |
| `kafka.batch.size` | `16384` | Producer `batch.size`（字节） |
| `kafka.compression.type` | `lz4` | Producer `compression.type` |
| `kafka.client.id` | `mongo-sync` | Producer `client.id` |
| `kafka.producer.*` | — | **透传** Producer 配置，写入时去掉 `kafka.producer.` 前缀。如 `kafka.producer.security.protocol=SASL_SSL` |

topic 拼接规则：

```text
{kafka.topic.prefix}{kafka.topic.separator}{db}{kafka.topic.separator}{collection}{kafka.topic.suffix}

例：prefix=mongo, separator=., db=demo, coll=orders, suffix 为空
  → mongo.demo.orders
```

## 十三、HTTP 控制面（仅配置文件启动时可用）

`SyncMain` 支持启一个轻量 HTTP 控制面，用于不重启进程地查询进度与干预：

| 键 | 默认 | 说明 |
|----|------|------|
| `http.enabled` | `false` | 是否启用 |
| `http.host` | `127.0.0.1` | 监听地址 |
| `http.port` | `27182` | 监听端口 |

端点与调用示例见 [SDK 与控制面 · HTTP 控制面](api.md#http-控制面)。

> 默认只监听 `127.0.0.1`。若改为对外监听，请自行加访问控制 —— 接口没有鉴权。

---

## 启动校验一览

进程启动时会先做一轮校验，不通过就退出（不会「跑起来再看」）：

| 校验 | 失败原因 |
|------|----------|
| `source.uri` / `sink.uri` 非空 | `MSYNC_CFG_001` |
| 单表必须有 `source.database` + `source.collection` | `MSYNC_CFG_001` |
| 多表模式不得混入 `source.database` / `source.collection` | `MSYNC_CFG_002` |
| 枚举值合法：`capture.mode` / `sync.mode` / `full.document` / `write.mode` / `on.conflict` / `sink.type` / `kafka.output.format` | `MSYNC_CFG_002` |
| `progress.log.interval.seconds >= 0` | `MSYNC_CFG_002` |
| `sink.batch.size` / `sink.writer.threads` / `bucket.num` 均 > 0 | `MSYNC_CFG_002` |
| 未携带已移除键 `source.oplog.uris` / `source.oplog.shard.names` | `MSYNC_CFG_002` |
| 命令行参数均为已知参数 | `MSYNC_ARG_001` |
| 配置文件存在 | `MSYNC_FILE_001` |

---

下一步：[同步模式](sync-modes.md) · [SDK 与控制面](api.md) · [运行与排障](operations.md)
