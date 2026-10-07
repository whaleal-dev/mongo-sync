# 运行与排障

## 部署形态

| 形态 | 做法 | 适用 |
|------|------|------|
| 脚本守护 | `bin/mongosync.sh -f conf.properties`，配合 systemd / supervisor | 独立同步进程（推荐） |
| 嵌入式 SDK | 在业务服务里 `MongoSyncClient.create(...).start()` | 与业务共进程、统一事件模型 |
| 容器 / 分发包 | `bin/package.sh` 产出的 `dist/` 或 `tar.gz` | 交付给运维 |

### 脚本进程管理要点

| 项 | 说明 |
|----|------|
| 单实例约束 | 同一份配置文件只允许一个实例（pid 文件位于 `run/mongosync-<hash>.pid`） |
| 优雅停止 | Ctrl+C、`kill -TERM`、`./bin/mongosync.sh --config <file> --shutdown` |
| 停止时发生什么 | 停捕获 → 排空在途写入 → 关闭资源；状态转 `STOPPED` |
| 内存 | `JAVA_OPTS` 可覆盖，默认 `-Xms512m -Xmx2g`（校验 `-Xms256m -Xmx1g`） |
| 日志 | 全部走 stderr（包含 `progress`、位点、`WINDOW WARN`、写失败） |

systemd 单元示例：

```ini
[Service]
WorkingDirectory=/opt/mongo-sync
Environment=JAVA_OPTS=-Xms1g -Xmx4g
ExecStart=/opt/mongo-sync/bin/mongosync.sh -f /opt/mongo-sync/conf/mongo-sync.properties
ExecStop=/opt/mongo-sync/bin/mongosync.sh -f /opt/mongo-sync/conf/mongo-sync.properties --shutdown
Restart=on-failure
RestartSec=10
```

> `--shutdown` 依赖 pid 文件里记录的进程号。若用容器且 PID 命名空间隔离，请确认 `run/` 目录挂载位置一致。

---

## 看懂 progress 日志

`progress.log.interval.seconds`（默认 10 秒）会周期性打印一行：

```text
[mongo-sync] progress ns=demo.orders phase=INITIAL_COPY state=RUNNING mode=FULL_AND_INCREMENTAL \
  capture=CHANGE_STREAM topology=REPLICA_SET fullCopied=120000 fullEstimated=1000000 \
  fullPercent=12 fullRemaining=880000 incrEvents=5231 ddlEvents=2 inflight=37 \
  canCommit=false readiness=waiting full sync completion incrPaused=false \
  windowRemainingSec=3431 elapsedMs=1830000
```

| 字段 | 怎么读 |
|------|--------|
| `phase` | `INITIAL_COPY` = 全量阶段；`CHANGE_EVENT_APPLY` = 全量完成、正在消费增量；`READY_TO_COMMIT` = 可以切流 |
| `capture` / `topology` | `AUTO` 展开后的实际通道与探测到的源端拓扑 |
| `fullCopied` / `fullPercent` / `fullRemaining` | 全量进度（`fullEstimated` 来自 `estimatedDocumentCount`，是估算值） |
| `incrEvents` | 已消费增量事件数；停在原地不涨通常说明源端没写入 |
| `inflight` | 在途事件数。持续高位说明写入跟不上（可加 `sink.writer.threads` 或排查目标端） |
| `lagMs` | 增量滞后。持续增长 = 追不上 |
| `canCommit` / `readiness` | 能否切流；`readiness` 给出**没满足的原因** |
| `incrPaused` | 是否处于「只冻增量」状态 |
| `windowRemainingSec` | 捕获窗口余量（秒）。**持续下降且逼近 0 是危险信号** |

### 三条健康红线

| 现象 | 含义 | 处置 |
|------|------|------|
| `windowRemainingSec` 逼近 0，或出现 `WINDOW WARN` | 源端 oplog 快被覆盖，增量即将断链 | 提高全量并发（`full.sync.parallelism`）、扩大源端 oplog 容量、或改小同步范围尽快完成全量 |
| `lagMs` 持续增长 | 增量追不上源端写入 | 排查目标端写入瓶颈（`sink.batch.size` / `sink.writer.threads` / 网络）、必要时先 `pauseIncremental` 减压 |
| `inflight` 长期不归零 | 有写入卡住或持续失败 | 检查写失败回调与日志（`write-error`），确认目标端可用 |

---

## 位点与断点续传

| 配置 | 行为 |
|------|------|
| 未配 `offset.store.dir` | 位点仅在内存 ⇒ **进程重启从头开始** |
| 配了 `offset.store.dir` | 位点按命名空间落文件，重启从上次位置继续 |

`offset.log.interval.seconds`（默认 30 秒）会周期打印当前位点，便于事后判断「最后一次成功同步到哪」：

```text
[mongo-sync] offset ns=demo.orders ts=1759820000:12 resumeToken=...
```

> ⚠️ 位点是在 **Source 回调后**保存，写入是异步的。因此崩溃时可能丢「已记位点但没落地」的事件。这是已知限制（见下）；严格场景请配合 [数据校验](verify.md) 复核。

---

## 故障排查

| 现象 | 原因 | 处理 |
|------|------|------|
| 启动报 `MSYNC_CFG_002 ... source.oplog.uris has been removed` | 配置里残留已移除键（多分片 OPLOG 已下线） | 删除 `source.oplog.uris` / `source.oplog.shard.names`，改用 `capture.mode=CHANGE_STREAM` 连 mongos，或**每个 shard 副本集各跑一个任务** |
| `capture.mode=OPLOG` 启动失败 | Oplog 不能用于 mongos / standalone | 副本集用 `CHANGE_STREAM`（或 `AUTO`）；mongos 用 `CHANGE_STREAM`；standalone 只能 `FULL` |
| `mongo.version` ≥ 7.0 配了 OPLOG | MongoDB ≥ 7.0 已移除 oplog，启动期校验拦截 | 改用 `capture.mode=CHANGE_STREAM` |
| 报「multi-sync uses namespace.white/black; do not mix source.database」 | 多表模式里混了单表键 | 二选一：用白黑名单，或删掉 `namespace.*` 用单表键 |
| 报「namespace white and black list cannot both be set」 | 白名单与黑名单同时非空 | 只保留一个 |
| 报 `MSYNC_ARG_001 unknown arg` | 命令行参数拼错 | 对照 [快速开始 · 命令行覆盖与简参启动](../getting-started/quickstart.md#命令行覆盖与简参启动) 的参数列表 |
| `mongosync.sh` 退出码 `20` | 同一份配置已有实例在运行 | 先 `--shutdown`，或确认 pid 文件指向的进程 |
| 退出码 `21` / `22` | pid 文件不存在 / 已失效 | 检查 `run/` 目录与配置文件路径是否变化（pid 文件名由配置**绝对路径**的校验和决定，换路径就换 pid 文件） |
| 写失败只在 stderr 里看到，没人知道 | 未注入 `SyncWriteErrorHandler`，且**不会自动重试** | 注入回调并接入告警，见 [SDK 与控制面 · 写失败回调](api.md#写失败回调) |
| 重启后从头再跑一次全量 | 未配 `offset.store.dir` | 配置位点持久化目录 |
| `commit()` 被拒 | 未满足 `CAN_COMMIT` | 看异常里的 `readiness`：`waiting full sync completion` / `waiting inflight drain=N` / `waiting lagMs=… maxLagMs=…` / `waiting first incremental event` |
| `pause()` 报错 | 初始全量进行中，或状态为 `COMMITTING` / `COMMITTED` | 全量期间用 `pauseIncremental()` |
| `resumeIncremental()` 报错 | 整体处于 `PAUSED` | 改用 `resume()` |
| Kafka 形态下 bootstrap 被跳过 | `bootstrap.collection` / `bootstrap.indexes` 在 Kafka 下无意义 | 正常行为，日志会明确提示 |
| 校验显示同步「已完成」但有差异 | 自动维护字段差异、校验期间有并发写入、或位点未持久化导致崩溃重跑留下缺口 | 用 `verify.ignore.fields` 排除自动字段；大流量期间接受瞬时差异；补齐位点持久化后重跑 |

---

## 能力边界与已知限制

来自 `mongo-sync` 的架构审查（完整清单见 [ARCHITECTURE.md](../ARCHITECTURE.md) §4）：

| 级别 | 限制 | 说明 |
|------|------|------|
| 中 | 写失败默认仅 stderr，**不自动重试** | 生产必须注入 `SyncWriteErrorHandler` |
| 中 | 位点在 Source 回调后即保存 | Sync 异步写入时，崩溃可能丢未落库事件；严格场景需自行校验 |
| 中 | ChangeStream 为集合级 watch | 多表时每表一条 watch；`dropDatabase` 等**库级事件可能收不全**；Oplog 更完整 |
| 中 | 分片 orphan / balancer | 未做 orphan 文档过滤；`include.from.migrate` 可控制是否包含迁移事件 |
| 低 | MongoDB ≥ 7.0 禁 Oplog | 已在启动期校验并拦截 |
| 低 | 同 ns 跨批异步写 | 默认 8 个写线程；同 `_id` 靠分桶内 `flushAndWait` 保序，不同 ns 可并发 |
| 低 | 唯一索引仅启动时探测一次 | 运行中新建的唯一索引不会改变分桶策略；需重启生效 |

> 判断是否适合你的场景时，上表比 README 的「核心能力」更有参考价值。

---

## 常见问题（FAQ）

### 支持哪些数据同步方式？

全量复制、实时/增量同步、自定义同步范围（多库表白黑名单 + 命名空间变换）以及复合方案（`FULL_AND_INCREMENTAL` / `FULL_THEN_CATCH_UP`）。

### 同步性能如何？

按 `_id` 分桶 + LMAX Disruptor 背压保证有序与吞吐；大表全量可 `full.sync.parallelism > 1` 切段并行读；写入侧 `sink.batch.size` / `sink.writer.threads` 可调；配了 `offset.store.dir` 即支持断点续传。

### 要同步 MySQL / Oracle / PostgreSQL 用哪个？

用姊妹产品 [rds-sync](https://github.com/whaleal-dev/rds-sync)。本仓只做文档库（MongoDB / 协议兼容库 / Kafka Sink）。

### 数据校验能保证什么？

`FULL` 模式逐文档比对 `_id` 与内容（可忽略字段），可覆盖「总量一致」「信息一致」「索引一致」的常规验收；但它**不是快照读**，高并发写入期间存在瞬时差异，属正常。

### 源端是 DocumentDB / 阿里云 DDS，能用吗？

可以（标准 Mongo 驱动写协议兼容库）。但兼容库在部分算子与 DDL 上可能与社区版有差异，**迁云前务必验证**，尤其是索引与 DDL 相关行为。

### 全量跑太久，oplog 会不会被覆盖？

会有风险。这正是 `window.warn.seconds`（默认 3600）存在的原因 —— 逼近阈值会打 `WINDOW WARN`。可同时提高全量并发、扩大源端 oplog 容量、或缩小同步范围。

---

## 生产上线清单

- [ ] `offset.store.dir` 已配置（否则重启不续传）
- [ ] 已注入 `SyncWriteErrorHandler` 并接入告警（否则写失败无人知）
- [ ] `sync.mode` 显式设置（SDK 默认是 `INCREMENTAL`，不会做全量）
- [ ] `capture.mode` 用 `AUTO`，并确认启动日志里的 `resolvedCapture` 符合预期
- [ ] `window.warn.seconds` 已按全量预期耗时调整，并有人盯 `WINDOW WARN`
- [ ] `write.mode=UPSERT`（不停服场景必须，用于覆盖全量与增量的重叠窗口）
- [ ] 唯一二级索引冲突策略 `on.conflict` 已确认（`FAIL` / `SKIP` / `UPSERT`）
- [ ] 分片源端已确认 `include.from.migrate` 取值符合预期
- [ ] 监控接入了 `lagMs` / `inflight` / `canCommit` / `windowRemainingSeconds`
- [ ] 切换流程已明确：`canCommit` → `commit()`（或 `commit.when.ready=true`）
- [ ] 切换前用 `verify.sh`（`FULL`）完成一次验收
- [ ] 停止方式已验证：`--shutdown` / `SIGTERM` 能优雅退出，排空后进程结束
- [ ] 权限：源端需能读 `local.oplog.rs`（Oplog 模式）或 watch（ChangeStream）；目标端需写入与建索引权限

---

返回：[手册首页](../README.md) · QQ 交流群：**983986505**
