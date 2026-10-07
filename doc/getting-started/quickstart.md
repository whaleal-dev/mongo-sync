# 快速开始

## 环境要求

| 项 | 要求 |
|----|------|
| JDK | **1.8+** |
| 构建 | Maven 3.6+ |
| 源端 MongoDB | 全量：任意形态；增量：副本集 ≥ 3.6（或分片 ≥ 3.6 走 ChangeStream@mongos） |
| 目标端 | MongoDB 实例，或 Kafka（`sink.type=kafka`） |
| 可选 | Kafka 集群（仅 Kafka Sink 需要） |

---

## 方式一：脚本启动（推荐先试这个）

脚本无需先打包 fat jar —— 没有 `dist/lib/mongo-sync-all.jar` 时会自动回落到 `mvn exec:java`。

```bash
cd mongo-sync
chmod +x bin/*.sh

# 1) 准备配置
cp doc/examples/mongo-sync.example.properties my-sync.properties
#    编辑 my-sync.properties：至少填 source.uri / sink.uri /
#    source.database / source.collection（或 namespace.white）

# 2) 启动同步
./bin/mongosync.sh -f my-sync.properties
#    Ctrl+C 优雅停止；也可另开终端执行：
#    ./bin/mongosync.sh --config my-sync.properties --shutdown

# 3) 数据比对（另开终端 / 同步稳定后）
./bin/verify.sh -f doc/examples/mongo-verify.example.properties
```

常用变体：

```bash
# 不指定配置，使用默认示例
./bin/mongosync.sh

# 开 progress 日志（每 5 秒）+ 达到 canCommit 后自动 commit 并退出
./bin/mongosync.sh --progress-log-seconds 5 --commit-when-ready -f my-sync.properties

# 用环境变量传配置路径
CONF=./my-sync.properties ./bin/mongosync.sh
```

脚本参数一览（`./bin/mongosync.sh --help`）：

| 参数 | 说明 |
|------|------|
| `-f, --config FILE` | 指定配置文件 |
| `--shutdown` | 优雅关闭该配置对应的实例（按 pid 文件发 `TERM`） |
| `-h, --help` | 帮助 |
| 其他参数 | 原样透传给 `SyncMain`（见下方「命令行覆盖」） |

### 脚本的进程管理

| 行为 | 说明 |
|------|------|
| pid 文件 | `run/mongosync-<配置文件绝对路径的 cksum>.pid` —— 同一份配置同时只能跑一个实例 |
| 重复启动 | 已有存活实例 → 退出码 `20` |
| `--shutdown` 找不到 pid 文件 | 退出码 `21` |
| `--shutdown` 发现失效 pid（进程已不在） | 清理 pid 文件并退出码 `22` |
| 参数错误 | 退出码 `2` |
| JVM 参数 | 同步 `JAVA_OPTS` 默认 `-Xms512m -Xmx2g`；校验默认 `-Xms256m -Xmx1g` |

---

## 方式二：Maven 直接启动

```bash
# 同步
mvn -q -pl mongo-sync-client -am package -DskipTests exec:java \
  -Dexec.mainClass=com.whaleal.third.mongo.sync.launcher.SyncMain \
  -Dexec.args="doc/examples/mongo-sync.example.properties"

# 校验
mvn -q -pl mongo-sync-client -am package -DskipTests exec:java \
  -Dexec.mainClass=com.whaleal.third.mongo.sync.verify.VerifyMain \
  -Dexec.args="doc/examples/mongo-verify.example.properties"
```

### 命令行覆盖与简参启动

`SyncMain` 支持在配置文件之外用命令行覆盖部分键（也可完全不用配置文件）：

```bash
java -cp dist/lib/mongo-sync-all.jar com.whaleal.third.mongo.sync.launcher.SyncMain \
  --source-uri 'mongodb://src:27017/?replicaSet=rs0' \
  --sink-uri 'mongodb://tgt:27018' \
  --source-db demo --source-coll orders \
  --sync-mode FULL_AND_INCREMENTAL \
  --capture-mode AUTO
```

支持的命令行参数：`--config`、`--source-uri`、`--sink-uri`、`--source-db`、`--source-coll`、`--sink-db`、`--sink-coll`、`--namespace-white`、`--sync-mode`、`--capture-mode`、`--commit-when-ready`、`--progress-log-seconds`。

> 传入未知参数会直接失败（错误码 `MSYNC_ARG_001`），不会静默忽略 —— 这是刻意的：拼错参数比报错更危险。

---

## 方式三：嵌入式 SDK

### 单表

```java
MongoSyncClient sync = MongoSyncClient.create(MongoSyncClient.builder()
        .sourceUri("mongodb://src/?replicaSet=rs0")
        .sinkUri("mongodb://sink")
        .mapCollection("demo", "orders")          // 源 = 目标，同名
        .captureMode(CaptureMode.AUTO)
        .syncMode(SyncMode.FULL_AND_INCREMENTAL)
        .offsetStoreDir("./data/offsets")         // 生产务必配置，否则重启不续传
        .writeErrorHandler((bucket, event, err) -> {
            // 生产务必处理写失败：落盘 / 告警 / 计数
        }));

sync.start();
// ... 业务运行中可查询与干预
MigrationProgress p = sync.progress();
// 切换前：
if (sync.canCommit()) {
    sync.commit();
}
sync.close();
```

### 多库表

```java
MongoMultiSyncClient multi = MongoMultiSyncClient.create(MongoMultiSyncConfig.builder()
        .sourceUri("mongodb://src/?replicaSet=rs0")
        .sinkUri("mongodb://sink")
        .namespaceWhite("demo;app.orders")        // 整库 demo + 单集合 app.orders
        .namespaceTransform("demo.orders:backup.orders_copy")
        .syncMode(SyncMode.FULL_AND_INCREMENTAL)
        .offsetStoreDir("./data/offsets")
        .writeErrorHandler((bucket, event, err) -> { }));

multi.start();
```

> `namespace.white` 与 `namespace.black` **互斥**，两者都填会直接抛错（与 MongoShake 一致）。

### Kafka Sink

```java
MongoSyncClient.create(MongoSyncClient.builder()
        .sourceUri("mongodb://src/?replicaSet=rs0")
        .sinkUri("127.0.0.1:9092")                // bootstrap servers
        .sinkType(SinkType.KAFKA)
        .mapCollection("demo", "orders")
        .kafkaTopicPrefix("mongo")                // topic: mongo.demo.orders
        .syncMode(SyncMode.FULL_AND_INCREMENTAL)
        .offsetStoreDir("./data/offsets")
        .writeErrorHandler((bucket, event, err) -> { }));
```

> Kafka 形态下 `bootstrap.collection` / `bootstrap.indexes` 会被跳过并打日志 —— Kafka 没有集合与索引可建。

---

## 打包发布（可选）

需要交付 fat jar + 脚本 + 配置模板时：

```bash
./bin/package.sh
```

产物：

```text
dist/
├── bin/                     mongosync.sh / verify.sh / README.md
├── conf/                    mongo-sync.properties / mongo-verify.properties
├── lib/                     mongo-sync-all.jar（shaded fat jar）
├── data/                    位点等运行期数据目录
├── mongo-sync-<version>/    可分发目录（含 README）
└── mongo-sync-<version>.tar.gz
```

打包后 `./bin/mongosync.sh` / `./bin/verify.sh` 会**优先使用** `dist/lib/mongo-sync-all.jar`，不再走 Maven。

> `dist/conf/*.properties` 是 `bin/package.sh` 从 `doc/examples/` 复制来的模板，改配置请改自己的副本，别改模板。

---

## 校验退出码

| 退出码 | 含义 |
|--------|------|
| `0` | 全部通过 |
| `1` | 存在差异 |
| `2` | 参数 / 运行错误 |

可直接接入 CI 或切换脚本。详见 [数据校验](../guide/verify.md)。

---

## 下一步

- 把配置填完整 → [配置项全解](../guide/configuration.md)
- 选对同步模式 → [同步模式](../guide/sync-modes.md)
- 上线前过一遍 → [运行与排障 · 生产清单](../guide/operations.md#生产上线清单)
