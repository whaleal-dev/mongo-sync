# mongo-source-client

[中文](#mongo-source-client) | [English](#english)

一款轻量级嵌入式 Java CDC SDK，支持 MongoDB **ChangeStream** 与 **Oplog** 两种捕获方式，专为 Java 应用设计。

> 本模块属于父工程 [`mongo-sync`](../README.md)。产出通用 [`TransferEvent`](../mongo-transfer-model)（捕获无关）；Sink 只认该模型：`sink.write(event)` / `sink.applyDdl(ddl)`。

## 核心特性

- **纯嵌入式 SDK**：零中间件依赖，可直接嵌入业务服务，无需部署 Kafka Connect/Flink
- **双捕获模式**：ChangeStream（默认）与 Oplog（&lt;7.0），统一输出 `TransferEvent`（op/before/after，对齐 Sink）
- **版本显式声明**：Oplog 模式须客户端传入 `mongoVersion`，按 V1/V2/V3 解析（对齐 d2t）
- **DDL 可选出口**：索引/DDL 经 `ddlListener` 回调，未配置则只推进位点
- **位点**：进程内内存 + 周期心跳日志；**当前不做持久化**
- **生产级容错**：完善重连、断线自动重试、oplog 窗口丢失检测

## 能力边界

### 包含能力

- ChangeStream 实时增量监听（副本集 / 分片）；CRUD + DDL（create/drop/createIndexes/dropIndexes/rename/dropDatabase，需 `ddlListener`）
- Oplog 直读（MongoDB **3.2–6.0**）：CRUD + 同上 DDL 经 `ddlListener`
- `fullDocument` 可选（DEFAULT 透传增量 / UPDATE_LOOKUP 回表等）
- V1 `system.indexes` 建索引归一化为 `createIndexes`
- V3（5.0/6.0）update `diff` 归一为 `$set/$unset`
- 位点（ResumeToken / oplog ts）SPI、`SyncMode` 全量∥增量并行、优雅启停

### SyncMode

| 模式 | 说明 |
|------|------|
| `FULL` | 仅全量 |
| `FULL_AND_INCREMENTAL` | 全量与增量**并行**，之后持续增量 |
| `FULL_AND_CATCH_UP` | 全量与增量**并行**，全量结束后追平上界再停 |
| （并行中删表/改名） | 增量识别本表 `DROP` / `RENAME` / 本库 `dropDatabase` 时，源端全量扫描**视为完成**并退出 |
| `INCREMENTAL` | 仅增量 |

详见 [mongo-sync-client/README.md](../mongo-sync-client/README.md)。

### 排除能力

- ❌ MongoDB **≥ 7.0 禁止 OPLOG**，须使用 ChangeStream
- ❌ 本模块不含写入落地（见同工程 `mongo-sink-client`）
- ❌ **当前不做位点持久化**（仅内存；靠周期日志观察上次同步时间）
- ❌ 不做 `buildInfo` 自动探测版本（Oplog 必须显式传入）
- ❌ 不提供独立启动 Jar、命令行工具、Web 管理界面
- ❌ Source 单客户端仍单集合；多表请用上层 `MongoMultiSyncClient` 编排

## 捕获模式与版本

| MongoDB | OplogFormat | OPLOG | CHANGE_STREAM |
|---------|-------------|-------|---------------|
| 3.2 / 3.4 | V1 | ✅ | ✅ |
| 3.6 / 4.0 / 4.4 | V2 | ✅ | ✅ |
| 5.0 / 6.0 | V3 | ✅ | ✅ |
| ≥ 7.0 | — | ❌ | ✅（强制） |

## 技术栈

- Java 8+
- MongoDB 官方同步驱动 `mongodb-driver-sync` 4.11.1
- Jackson（可选，用于标准事件 JSON 序列化）

## 快速开始

### Maven 依赖

```xml
<dependency>
    <groupId>com.whaleal.third</groupId>
    <artifactId>mongo-source-client</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### ChangeStream（默认）

```java
import com.whaleal.third.mongo.source.sdk.MongoSourceClient;
import com.whaleal.third.mongo.source.config.MongoSourceConfig;
import com.whaleal.third.mongo.source.spi.ResumeTokenStorage;

ResumeTokenStorage resumeTokenStorage = new CustomResumeTokenStorage();

MongoSourceClient client = MongoSourceClient.builder()
        .uri("mongodb://127.0.0.1:27017/?replicaSet=rs0")
        .database("test_db")
        .collection("test_coll")
        .fullDocument(MongoSourceConfig.FullDocumentMode.UPDATE_LOOKUP)
        .enablePreImage(true)
        .syncMode(SyncMode.FULL_AND_INCREMENTAL)
        .resumeTokenStorage(resumeTokenStorage)  // 位点 = ResumeToken
        .listener(event -> {
            System.out.println("Operation: " + event.getOp());
            System.out.println("After: " + event.getAfter());
        })
        .build();

client.start();
```

### Oplog（须显式传入 mongoVersion）

```java
import com.whaleal.third.mongo.source.config.CaptureMode;
import com.whaleal.third.mongo.source.config.MongoSourceConfig;
import com.whaleal.third.mongo.source.sdk.MongoSourceClient;
import com.whaleal.third.mongo.source.spi.OplogOffsetStorage;

OplogOffsetStorage oplogOffsetStorage = new CustomOplogOffsetStorage();

MongoSourceClient client = MongoSourceClient.builder()
        .uri("mongodb://127.0.0.1:27017/?replicaSet=rs0")
        .database("test_db")
        .collection("test_coll")
        .captureMode(CaptureMode.OPLOG)
        .mongoVersion("4.4.29")   // 必填；≥7.0 会构建失败
        .fullDocument(MongoSourceConfig.FullDocumentMode.DEFAULT) // 可选；也可 UPDATE_LOOKUP
        .oplogOffsetStorage(oplogOffsetStorage) // 位点 = oplog ts 时间戳
        .listener(event -> { /* CRUD TransferEvent */ })
        .ddlListener(ddl -> {   // 可选；索引/DDL
            System.out.println(ddl.getType() + " " + ddl.getCommand());
        })
        .build();

client.start();
```

## 位点模型（务必区分）

| 捕获模式 | 位点类型 | SPI | 含义 |
|----------|----------|-----|------|
| CHANGE_STREAM | `ResumeToken` | `ResumeTokenStorage` | ChangeStream resume token（BsonDocument） |
| OPLOG | `OplogOffset` | `OplogOffsetStorage` | `local.oplog.rs` 的 `ts`（BsonTimestamp） |

二者**不可混用**；构建时若传错 SPI 会直接失败。

## API 文档

### MongoSourceClient.Builder

| 方法 | 参数 | 说明 | 默认值 |
|------|------|------|--------|
| `uri(String)` | MongoDB 连接 URI | 与 `mongoClient` 二选一 | - |
| `mongoClient(MongoClient)` | 复用外部连接（推荐，常驻连接池） | 与 `uri` 二选一；注入后 stop 默认不关 | null |
| `closeMongoClientOnStop(boolean)` | stop 时是否关闭 client | 外部注入默认 false；uri 自建默认 true | - |
| `database(String)` | 数据库名称 | 必填 | - |
| `collection(String)` | 集合名称 | 必填 | - |
| `captureMode(CaptureMode)` | CHANGE_STREAM / OPLOG | 可选 | CHANGE_STREAM |
| `mongoVersion(String)` | 服务端版本，如 `4.4.29` | **OPLOG 必填**；≥7.0 禁止 OPLOG | - |
| `fullDocument(FullDocumentMode)` | fullDocument 模式（Oplog 下可选 lookup） | 可选 | DEFAULT |
| `enablePreImage(boolean)` | 是否开启 preImage（仅 ChangeStream） | 可选 | false |
| `pipeline(List<BsonDocument>)` | 自定义聚合过滤管道（仅 ChangeStream） | 可选 | null |
| `retryMaxTimes(int)` | 最大重试次数 | 可选 | 10 |
| `retryIntervalMs(long)` | 重试间隔（毫秒） | 可选 | 1000 |
| `offsetLogIntervalSeconds(int)` | 周期性打印位点到 stderr（秒）；`<=0` 关闭 | 可选 | 30 |
| `syncMode(SyncMode)` | FULL / FULL_AND_INCREMENTAL / FULL_AND_CATCH_UP / INCREMENTAL | 可选 | INCREMENTAL |
| `resumeTokenStorage(ResumeTokenStorage)` | ChangeStream 位点（ResumeToken） | 可选 | null |
| `oplogOffsetStorage(OplogOffsetStorage)` | Oplog 位点（时间戳） | 可选 | null |
| `listener(TransferEventListener)` | CRUD 事件回调 | 必填 | - |
| `ddlListener(DdlEventListener)` | 索引/DDL 回调（可选；未设则只推进位点） | 可选 | null |
| `oplogStartTimestamp(BsonTimestamp)` | Oplog 起始 ts（无存储位点时） | 可选 | 最新 ts |
| `oplogBatchSize(int)` | Oplog 游标 batchSize | 可选 | 1024 |
| `includeFromMigrate(boolean)` | 是否包含 fromMigrate 条目 | 可选 | false |
| `listenerThreadPriority(int)` | 监听线程优先级 | 可选 | NORM_PRIORITY |

### FullDocumentMode

| 枚举值 | 说明 |
|--------|------|
| `DEFAULT` | ChangeStream：无 fullDocument 时把 updateDescription 归一为 `$set/$unset`；Oplog：透传 `$set`/`diff` 归一结果，不回表 |
| `UPDATE_LOOKUP` | 更新时按 documentKey 回表查完整文档 |
| `WHEN_AVAILABLE` | 回表；找不到则回退透传 |
| `REQUIRED` | 必须回表到完整文档，否则报错 |

### TransferEvent（标准输出事件，与 Sink 对齐）

| 字段 | 类型 | 说明 |
|------|------|------|
| `before` | `Map<String, Object>` | delete：preImage 或 documentKey；update：preImage 或 documentKey（供 Sink 定位 `_id`） |
| `op` | `String` | 操作类型：c(新增)/u(更新)/d(删除)/r(全量快照) |
| `after` | `Map<String, Object>` | insert/snapshot：全量文档；update：全量文档 **或** `$set/$unset` |
| `source` | `TransferSource` | 数据源信息（库名、集合名、集群时间戳） |
| `tsMs` | `Long` | 事件发生时间戳（毫秒级） |

联用 Sink：`sink.write(event.getOp(), event.getAfter(), event.getBefore())`。

### TransferSource（数据源信息）

| 字段 | 类型 | 说明 |
|------|------|------|
| `db` | `String` | 数据库名称 |
| `collection` | `String` | 集合名称 |
| `clusterTime` | `Long` | 集群时间戳（毫秒级） |

## SPI 扩展

### ResumeTokenStorage（ChangeStream 位点）

```java
public interface ResumeTokenStorage {
    ResumeToken load();
    void save(ResumeToken token);
}
```

### OplogOffsetStorage（Oplog 位点）

```java
public interface OplogOffsetStorage {
    OplogOffset load();
    void save(OplogOffset offset);
}
```

`OplogOffset` 包装 `BsonTimestamp`（oplog `ts`），与 `ResumeToken` 分离。

业务可自行实现 Redis、MySQL、本地文件、内存等任意位点存储方式。

### TransferEventListener（事件回调接口）

```java
public interface TransferEventListener {
    void onEvent(TransferEvent event);
}
```

### ResumeTokenStorageAdapter（适配器抽象类）

提供默认空实现，业务只需按需覆写：

```java
public class CustomOffsetStorage extends ResumeTokenStorageAdapter {
    @Override
    public ResumeToken load() {
        // 从 Redis/MySQL/文件加载位点
        return ResumeToken.empty();
    }

    @Override
    public void save(ResumeToken token) {
        // 保存到位点存储
    }
}
```

## 异常体系

| 异常类 | 触发场景 |
|--------|----------|
| `SourceConnectException` | MongoDB 连接、集群连接异常 |
| `TransferEventConvertException` | 事件标准化转换异常 |
| `SourceOffsetException` | 位点加载/保存异常 |
| `SourceHistoryLostException` | 位点过期、变更日志丢失异常 |

## 使用示例

### 示例 1：基础监听

```java
MongoSourceClient client = MongoSourceClient.builder()
        .uri("mongodb://localhost:27017")
        .database("mydb")
        .collection("mycoll")
        .listener(event -> {
            switch (event.getOp()) {
                case "c":
                    System.out.println("新增: " + event.getAfter());
                    break;
                case "u":
                    System.out.println("更新前: " + event.getBefore());
                    System.out.println("更新后: " + event.getAfter());
                    break;
                case "d":
                    System.out.println("删除: " + event.getBefore());
                    break;
                case "r":
                    System.out.println("快照: " + event.getAfter());
                    break;
            }
        })
        .build();

client.start();
```

### 示例 2：自定义位点存储

```java
public class RedisOffsetStorage implements ResumeTokenStorage {
    private final String key = "mongo-source-offset";

    @Override
    public ResumeToken load() {
        String tokenJson = redis.get(key);
        if (tokenJson == null) {
            return ResumeToken.empty();
        }
        BsonDocument token = BsonDocument.parse(tokenJson);
        return ResumeToken.fromBson(token);
    }

    @Override
    public void save(ResumeToken token) {
        if (!token.isEmpty()) {
            redis.set(key, token.getToken().toJson());
        }
    }
}
```

### 示例 3：开启全量快照同步

```java
MongoSourceClient client = MongoSourceClient.builder()
        .uri("mongodb://localhost:27017")
        .database("mydb")
        .collection("mycoll")
        .syncMode(SyncMode.FULL_AND_INCREMENTAL)
        .resumeTokenStorage(new RedisOffsetStorage())
        .listener(event -> {
            if ("r".equals(event.getOp())) {
                // 全量快照数据，初始化下游
            } else {
                // 增量变更数据
            }
        })
        .build();

client.start();
```

### 示例 4：自定义过滤管道

```java
List<BsonDocument> pipeline = Arrays.asList(
    new BsonDocument("$match", 
        new BsonDocument("operationType", new BsonString("insert"))
    )
);

MongoSourceClient client = MongoSourceClient.builder()
        .uri("mongodb://localhost:27017")
        .database("mydb")
        .collection("mycoll")
        .pipeline(pipeline)
        .listener(event -> {
            // 只接收 insert 事件
        })
        .build();
```

## 配置建议

### 生产环境配置

```java
MongoSourceClient client = MongoSourceClient.builder()
        .uri("mongodb://node1:27017,node2:27017,node3:27017/?replicaSet=rs0&readPreference=primary")
        .database("production_db")
        .collection("business_coll")
        .fullDocument(MongoSourceConfig.FullDocumentMode.UPDATE_LOOKUP)
        .enablePreImage(true)
        .syncMode(SyncMode.FULL_AND_INCREMENTAL)
        .retryMaxTimes(30)
        .retryIntervalMs(2000)
        .resumeTokenStorage(new ReliableOffsetStorage())
        .listener(new BusinessEventListener())
        .listenerThreadPriority(Thread.MAX_PRIORITY)
        .build();
```

## 注意事项

1. **MongoDB 版本要求**：需 MongoDB 3.6+，建议使用 4.0+
2. **权限要求**：连接用户需具备 `read` 权限及 `changeStream` 权限
3. **Oplog 大小**：确保 MongoDB Oplog 大小足够，避免位点过期
4. **preImage 配置**：使用 preImage 需在集合级别开启 `changeStreamPreAndPostImages`
5. **线程安全**：`MongoSourceClient` 线程安全，可单例使用

## 许可证

MIT License

---

<a id="english"></a>

# mongo-source-client

A lightweight embedded Java CDC SDK based on MongoDB ChangeStream, designed specifically for Java applications.

## Core Features

- **Pure Embedded SDK**: Zero middleware dependency, can be directly embedded into business services without deploying Kafka Connect/Flink
- **Industry Standard Alignment**: Native Debezium Envelope format output, connecting the full ecosystem CDC pipeline
- **Extreme Lightweight**: Minimal dependencies, no redundant features, no built-in Sink, focusing on core listening and standardization
- **Highly Flexible Decoupling**: SPI-based offset abstraction, allowing business to customize persistence logic without technology binding
- **Production-Grade Fault Tolerance**: Complete reconnection, cluster switchover, and exception fallback capabilities, suitable for production environments

## Capability Boundary

### Included Capabilities

- Real-time incremental listening based on MongoDB ChangeStream, compatible with replica sets and sharded clusters
- Complete connection management, master-slave switchover, automatic reconnection, exponential backoff retry fault tolerance mechanism
- Full ChangeStream parameter configuration (fullDocument, preImage, custom filter pipeline, etc.)
- Automatic conversion from native Bson change events to Debezium standard Envelope POJO
- Standardization of Mongo special data types (ObjectId, Timestamp, Decimal128, Date, etc.)
- SPI abstract definition of offset (ResumeToken), supporting custom persistence implementation
- Graceful startup/shutdown, resource release, exception classification capture and throwing
- Event enumeration normalization (insert/update/replace/delete unified standard op field)
- Built-in Initial Sync full snapshot synchronization capability

### Excluded Capabilities

- ❌ Does not support legacy Oplog direct reading mode, focusing only on ChangeStream
- ❌ Does not implement any Sink landing logic (Kafka, ES, MySQL, file writing, etc.)
- ❌ Does not provide specific offset persistence implementation (only SPI interface definition)
- ❌ Does not provide independent startup Jar, command-line tools, or Web management interface
- ❌ V1.0 does not support multi-collection parallel listening

## Tech Stack

- Java 8+
- MongoDB official synchronous driver `mongodb-driver-sync` 4.11.1
- Jackson (optional, for standard event JSON serialization)

## Quick Start

### Maven Dependency

```xml
<dependency>
    <groupId>com.whaleal.third</groupId>
    <artifactId>mongo-source-client</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### Basic Usage

```java
import com.whaleal.third.mongo.source.sdk.MongoSourceClient;
import com.whaleal.third.mongo.source.spi.ResumeTokenStorage;
import com.whaleal.third.mongo.transfer.model.TransferEvent;

ResumeTokenStorage resumeTokenStorage = new CustomOffsetStorage();

MongoSourceClient client = MongoSourceClient.builder()
        .uri("mongodb://127.0.0.1:27017/?replicaSet=rs0")
        .database("test_db")
        .collection("test_coll")
        .fullDocument(MongoSourceConfig.FullDocumentMode.UPDATE_LOOKUP)
        .enablePreImage(true)
        .syncMode(SyncMode.FULL_AND_INCREMENTAL)
        .retryMaxTimes(10)
        .resumeTokenStorage(offsetStorage)
        .listener(event -> {
            System.out.println("Operation: " + event.getOp());
            System.out.println("After: " + event.getAfter());
        })
        .build();

client.start();
```

## API Documentation

### MongoSourceClient.Builder

| Method | Parameter | Description | Default |
|--------|-----------|-------------|---------|
| `uri(String)` | MongoDB connection URI | Required | - |
| `database(String)` | Database name | Required | - |
| `collection(String)` | Collection name | Required | - |
| `fullDocument(FullDocumentMode)` | fullDocument mode | Optional | DEFAULT |
| `enablePreImage(boolean)` | Enable preImage | Optional | false |
| `pipeline(List<BsonDocument>)` | Custom aggregation filter pipeline | Optional | null |
| `retryMaxTimes(int)` | Max retry times | Optional | 10 |
| `retryIntervalMs(long)` | Retry interval (ms) | Optional | 1000 |
| `syncMode(SyncMode)` | FULL / FULL_AND_INCREMENTAL / FULL_AND_CATCH_UP / INCREMENTAL | Optional | INCREMENTAL |
| `resumeTokenStorage(ResumeTokenStorage)` | ChangeStream resume token storage | Optional | null |
| `listener(TransferEventListener)` | Event callback handler | Required | - |
| `listenerThreadPriority(int)` | Listener thread priority | Optional | NORM_PRIORITY |

### FullDocumentMode

| Enum Value | Description |
|------------|-------------|
| `DEFAULT` | Default mode |
| `UPDATE_LOOKUP` | Query full document on update |
| `WHEN_AVAILABLE` | Return full document only when available |
| `REQUIRED` | Must return full document, otherwise error |

### TransferEvent (Standard Output Event)

| Field | Type | Description |
|-------|------|-------------|
| `before` | `Map<String, Object>` | Data before change (effective for update/delete events) |
| `op` | `String` | Operation type: c(create)/u(update)/d(delete)/r(snapshot) |
| `after` | `Map<String, Object>` | Data after change (effective for insert/update events) |
| `source` | `TransferSource` | Data source info (database name, collection name, cluster timestamp) |
| `tsMs` | `Long` | Event timestamp (milliseconds) |

### TransferSource (Data Source Info)

| Field | Type | Description |
|-------|------|-------------|
| `db` | `String` | Database name |
| `collection` | `String` | Collection name |
| `clusterTime` | `Long` | Cluster timestamp (milliseconds) |

## SPI Extension

### ResumeTokenStorage (Offset Storage Interface)

```java
public interface ResumeTokenStorage {
    ResumeToken load();
    void save(ResumeToken token);
}
```

Business can implement any offset storage method such as Redis, MySQL, local file, memory, etc.

### TransferEventListener (Event Callback Interface)

```java
public interface TransferEventListener {
    void onEvent(TransferEvent event);
}
```

### ResumeTokenStorageAdapter (Adapter Abstract Class)

Provides default empty implementation, business only needs to override as needed:

```java
public class CustomOffsetStorage extends ResumeTokenStorageAdapter {
    @Override
    public ResumeToken load() {
        // Load offset from Redis/MySQL/File
        return ResumeToken.empty();
    }

    @Override
    public void save(ResumeToken token) {
        // Save to offset storage
    }
}
```

## Exception Hierarchy

| Exception Class | Trigger Scenario |
|-----------------|------------------|
| `SourceConnectException` | MongoDB connection, cluster connection exception |
| `TransferEventConvertException` | Event standardization conversion exception |
| `SourceOffsetException` | Offset load/save exception |
| `SourceHistoryLostException` | Offset expired, change log lost exception |

## Usage Examples

### Example 1: Basic Listening

```java
MongoSourceClient client = MongoSourceClient.builder()
        .uri("mongodb://localhost:27017")
        .database("mydb")
        .collection("mycoll")
        .listener(event -> {
            switch (event.getOp()) {
                case "c":
                    System.out.println("Create: " + event.getAfter());
                    break;
                case "u":
                    System.out.println("Before: " + event.getBefore());
                    System.out.println("After: " + event.getAfter());
                    break;
                case "d":
                    System.out.println("Delete: " + event.getBefore());
                    break;
                case "r":
                    System.out.println("Snapshot: " + event.getAfter());
                    break;
            }
        })
        .build();

client.start();
```

### Example 2: Custom Offset Storage

```java
public class RedisOffsetStorage implements ResumeTokenStorage {
    private final String key = "mongo-source-offset";

    @Override
    public ResumeToken load() {
        String tokenJson = redis.get(key);
        if (tokenJson == null) {
            return ResumeToken.empty();
        }
        BsonDocument token = BsonDocument.parse(tokenJson);
        return ResumeToken.fromBson(token);
    }

    @Override
    public void save(ResumeToken token) {
        if (!token.isEmpty()) {
            redis.set(key, token.getToken().toJson());
        }
    }
}
```

### Example 3: Enable Full Snapshot Sync

```java
MongoSourceClient client = MongoSourceClient.builder()
        .uri("mongodb://localhost:27017")
        .database("mydb")
        .collection("mycoll")
        .syncMode(SyncMode.FULL_AND_INCREMENTAL)
        .resumeTokenStorage(new RedisOffsetStorage())
        .listener(event -> {
            if ("r".equals(event.getOp())) {
                // Full snapshot data, initialize downstream
            } else {
                // Incremental change data
            }
        })
        .build();

client.start();
```

### Example 4: Custom Filter Pipeline

```java
List<BsonDocument> pipeline = Arrays.asList(
    new BsonDocument("$match", 
        new BsonDocument("operationType", new BsonString("insert"))
    )
);

MongoSourceClient client = MongoSourceClient.builder()
        .uri("mongodb://localhost:27017")
        .database("mydb")
        .collection("mycoll")
        .pipeline(pipeline)
        .listener(event -> {
            // Only receive insert events
        })
        .build();
```

## Configuration Recommendations

### Production Environment Configuration

```java
MongoSourceClient client = MongoSourceClient.builder()
        .uri("mongodb://node1:27017,node2:27017,node3:27017/?replicaSet=rs0&readPreference=primary")
        .database("production_db")
        .collection("business_coll")
        .fullDocument(MongoSourceConfig.FullDocumentMode.UPDATE_LOOKUP)
        .enablePreImage(true)
        .syncMode(SyncMode.FULL_AND_INCREMENTAL)
        .retryMaxTimes(30)
        .retryIntervalMs(2000)
        .resumeTokenStorage(new ReliableOffsetStorage())
        .listener(new BusinessEventListener())
        .listenerThreadPriority(Thread.MAX_PRIORITY)
        .build();
```

## Notes

1. **MongoDB Version Requirement**: MongoDB 3.6+ required, MongoDB 4.0+ recommended
2. **Permission Requirement**: Connection user needs `read` permission and `changeStream` permission
3. **Oplog Size**: Ensure MongoDB Oplog size is sufficient to avoid offset expiration
4. **preImage Configuration**: Using preImage requires enabling `changeStreamPreAndPostImages` at collection level
5. **Thread Safety**: `MongoSourceClient` is thread-safe and can be used as singleton

## License

MIT License