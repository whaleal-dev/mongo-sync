# mongo-sync

**MongoDB 文档同步 SDK / 工具**（Java 8+）

**主路径：MongoDB → MongoDB**（自建 / 云上均支持）。  
同构兼容目标亦可：Amazon **DocumentDB**、阿里云 **DDS** 等 MongoDB 协议兼容的文档库。

面向迁移、灾备、多活与跨架构搬迁：一套 API / 一条命令，完成 **全量 + 增量**、**DDL 跟随**、**多库表过滤** 与 **数据校验**。设计参考 d2t / MongoShake，并保持可嵌入 Java 业务进程的轻量形态。

```bash
./bin/mongosync.sh conf/mongo-sync.properties   # 启动同步
./bin/verify.sh    conf/mongo-verify.properties # 数据比对
```

**QQ 交流群：`983986505`**（使用问题、需求反馈、经验交流欢迎加群）

---

## 为什么选 mongo-sync

| 诉求 | mongo-sync 怎么做 |
|------|-------------------|
| Mongo → Mongo 主路径 | 全量∥增量、DDL、分桶有序写，覆盖迁库 / 灾备主场景 |
| 同构上云（DocumentDB / DDS） | 标准 Mongo 驱动写入协议兼容库，便于迁云 |
| 迁库 / 扩容不停服 | 全量∥增量并行（`FULL_AND_INCREMENTAL`），UPSERT 兜底窗口重复 |
| 跨架构互传 | 自动识别 standalone / 副本集 / 分片，匹配读任务（Oplog / ChangeStream） |
| 分片集群增量 | mongos 拉全量；各 shard 并行拉 Oplog（可自动 `listShards`） |
| 大表全量加速 | 按 `_id` 切段多任务并行读（对齐 d2t 拆分思路） |
| 结构一起走 | 启动预建集合 / 索引；运行中 DDL（删表、改名、建删索引）可落地 |
| 写序与吞吐 | `_id` 分桶 + LMAX Disruptor 背压；唯一索引自动有序写 |
| 可嵌入 / 可脚本 | SDK（`MongoSyncClient`）或 `mongosync.sh` 配置文件启动 |
| 迁完可验 | `verify.sh`：COUNT / ID / FULL 三种比对 |

Sink **不感知** 捕获协议——无论 Oplog 还是 ChangeStream，统一变成 `TransferEvent` / `DdlEvent` 再写入目标。

---

## 核心能力一览

- **四种同步模式**：仅全量、全量∥持续增量、全量∥追平后停、仅增量  
- **双捕获通道**：ChangeStream（推荐 / MongoDB 7.0+）；Oplog 3.2–6.0（V1/V2/V3 解析）  
- **架构自适应**：`capture.mode=AUTO` 按源端拓扑匹配读计划（禁止在 mongos / standalone 上误拉 Oplog）  
- **多库表**：白/黑名单、`ns` 变换（`MongoMultiSyncClient`）  
- **元数据**：`bootstrapCollection` / `bootstrapIndexes` 可分别开关；支持跳过 TTL 索引  
- **位点**：可选文件持久化（`offset.store.dir`）+ 周期心跳日志  
- **迁移状态机**：`MigrationProgress` / `canCommit` / `commit`（第一版）  
- **校验**：`VerifyMain` 支持单表 / 多表白名单  

---

## 源端读能力（简表）

| | 全量 | Oplog | ChangeStream |
|--|------|-------|--------------|
| standalone | ✅ | ❌ | ❌（仅 `FULL`） |
| 副本集 | ✅ | ✅ | ✅ |
| mongos | ✅ | ❌（改写为各 shard） | ✅ |
| 某 shard | — | ✅ | — |

---

## 30 秒上手

### 1. 脚本启动（推荐体验）

```bash
cd mongo-sync
chmod +x bin/*.sh
# 编辑配置：源/目标 URI、库表或 namespace.white
cp doc/examples/mongo-sync.example.properties my-sync.properties

./bin/mongosync.sh my-sync.properties
# Ctrl+C 优雅停止

./bin/verify.sh doc/examples/mongo-verify.example.properties
```

可选打包 fat jar：

```bash
./bin/package.sh
# → dist/lib/mongo-sync-all.jar + dist/conf/
```

### 2. 嵌入式 SDK

```java
MongoSyncClient sync = MongoSyncClient.create(MongoSyncClient.builder()
        .sourceUri("mongodb://src/?replicaSet=rs0")
        .targetUri("mongodb://target")
        .mapCollection("demo", "orders")
        .captureMode(CaptureMode.AUTO)
        .syncMode(SyncMode.FULL_AND_INCREMENTAL)
        .offsetStoreDir("./data/offsets")
        .writeErrorHandler((bucket, event, err) -> {
            // 生产务必处理写失败
        }));
sync.start();
```

多库表：

```java
MongoMultiSyncClient multi = MongoMultiSyncClient.create(MongoMultiSyncConfig.builder()
        .sourceUri("mongodb://src/?replicaSet=rs0")
        .targetUri("mongodb://target")
        .namespaceWhite("demo;app.orders")
        .syncMode(SyncMode.FULL_AND_INCREMENTAL)
        .offsetStoreDir("./data/offsets")
        .writeErrorHandler((bucket, event, err) -> { }));
multi.start();
```

---

## 模块结构

```text
mongo-sync/
├── mongo-transfer-model/   通用传输模型（捕获无关）
├── mongo-source-client/    Oplog / ChangeStream → TransferEvent
├── mongo-sink-client/      TransferEvent / DdlEvent → 目标库
├── mongo-sync-client/      编排：分桶 + Disruptor + 锁 → Sink
├── bin/                    mongosync.sh / verify.sh / package.sh
└── doc/                    架构说明、配置示例、oplog 样例
```

数据契约：

| 模型 | 含义 |
|------|------|
| `TransferEvent` | 文档变更 `c` / `u` / `d` / `r` |
| `DdlEvent` | 删库、删表、建删索引、建表、改名等 |

---

## 构建与依赖

```bash
mvn clean install -DskipTests
```

| 组件 | 版本要求 |
|------|----------|
| JDK | **1.8+** |
| Caffeine | **2.9.3**（勿用 3.x） |
| Disruptor | **3.4.4**（勿用 4.x） |
| mongodb-driver-sync | 4.11.1 |

```xml
<dependency>
  <groupId>com.whaleal.third</groupId>
  <artifactId>mongo-sync-client</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

---

## 文档

| 文档 | 说明 |
|------|------|
| [bin/README.md](bin/README.md) | 脚本入口详解 |
| [doc/ARCHITECTURE.md](doc/ARCHITECTURE.md) | 架构、能力清单与已知限制 |
| [doc/examples/mongo-sync.example.properties](doc/examples/mongo-sync.example.properties) | 同步配置示例 |
| [doc/examples/mongo-verify.example.properties](doc/examples/mongo-verify.example.properties) | 校验配置示例 |
| [doc/oplog/](doc/oplog/) | 各版本 Oplog 样例 |

---

## 适用场景

- **MongoDB → MongoDB**：迁库、扩容、跨机房 / 多活（主推）  
- **同构上云**：自建 Mongo → DocumentDB / DDS 等协议兼容库  
- **灾备与只读副本**：持续增量同步到备端  
- **架构升级**：副本集 ↔ 分片、跨版本（捕获通道随版本自动收紧）  
- **业务内嵌同步**：以 SDK 嵌入现有 Java 服务，统一事件模型  

> 生产请配置 `offset.store.dir`、注入 `SyncWriteErrorHandler`，切换前用 `verify.sh` 抽检。DocumentDB / DDS 与社区版在部分算子、DDL 上可能有差异，迁云前务必验证。更细限制见 [架构说明](doc/ARCHITECTURE.md)。

---

## 交流与支持

- QQ 交流群：**983986505**
