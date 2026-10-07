# 前言

## 为什么需要 mongo-sync

MongoDB 的搬迁与容灾，难点从来不是「把数据读出来再写进去」，而是：

- **迁库 / 扩容要不停服**：全量拷贝要几个小时，这期间业务还在写，靠停机窗口在多数场景里不可行
- **源端形态五花八门**：单节点、副本集、分片集群、云上 DocumentDB / 阿里云 DDS，读法完全不同
- **结构不只是数据**：集合、索引、运行中的建删索引与删表改名，漏掉就会在切换后暴露问题
- **搬完不敢切**：没有独立的数据比对手段，切过去出了差异只能靠人眼发现
- **重复投递**：CDC 链路天然会出现重复，写入端必须能幂等落地
- **顺序敏感**：同一文档的多次变更必须按序落地，否则最终状态是错的

mongo-sync 把这些收敛成 **一套统一的编排 + 一条命令**：

| 你关心的事 | mongo-sync 怎么做 |
|------------|-------------------|
| 不停服迁库 | `FULL_AND_INCREMENTAL`：全量与增量**并行**，全量结束后持续增量 |
| 源端形态自适应 | `capture.mode=AUTO` 按 `hello` / `isMaster` 探测拓扑，自动匹配读任务 |
| 结构一起走 | 启动预建集合 / 索引，运行中 DDL（建删索引、删表、改名）可落地 |
| 只同步部分库表 | `namespace.white` / `namespace.black` + `namespace.transform` 改目标命名空间 |
| 大表全量太慢 | 按 `_id` 切段多线程并行读（`full.sync.parallelism`） |
| 迁完要验收 | `verify.sh`：COUNT / ID / FULL 三种比对，退出码可直接接 CI |
| 重复投递 | 默认 `write.mode=UPSERT`，按 `_id` 幂等落地 |
| 同文档保序 | `_id` 分桶 + 每桶单写者 + LMAX Disruptor 背压 |
| 崩了能续 | `offset.store.dir` 持久化位点，重启从上次位置继续 |
| 迁完要切流 | `canCommit` / `commit`：全量完成、管道排空、增量滞后达标的**最小 cutover** |
| 嵌入自己的服务 | SDK（`MongoSyncClient` / `MongoMultiSyncClient`）或 `mongosync.sh` 配置文件启动 |

如果你觉得它省了你的时间，欢迎给仓库点 Star，也欢迎进 QQ 群交流：`983986505`。

## 产品特点

- **双捕获通道**：ChangeStream（推荐，MongoDB 7.0+ 唯一可选）与 Oplog（3.2–6.0，V1/V2/V3 解析）
- **双 Sink 形态**：MongoDB（默认）/ Kafka（消息对齐 mongo-kafka Source 的 Change Stream 格式）
- **架构自适应**：`AUTO` 禁止在 standalone / mongos 上误拉 Oplog
- **迁移状态机**：`MigrationProgress` 暴露相位、全量进度、增量计数、inflight、lag、能否提交
- **可观测**：周期位点日志、progress 日志、捕获窗口告警（oplog 快被覆盖时提前喊）
- **Java 8+**：Caffeine 2.9.3 / Disruptor 3.4.4 / kafka-clients 3.6.2，老环境也能用

## 适用场景

- **MongoDB → MongoDB**：迁库、扩容、跨机房 / 多活（主推）
- **MongoDB → Kafka**：把变更投递到 Kafka，格式对齐 mongo-kafka Source，下游可用 mongo-kafka Sink 消费
- **同构上云**：自建 Mongo → Amazon DocumentDB / 阿里云 DDS 等协议兼容库
- **灾备与只读副本**：持续增量同步到备端
- **架构升级**：副本集 ↔ 分片、跨版本搬迁（捕获通道随版本自动收紧）
- **业务内嵌同步**：以 SDK 嵌入现有 Java 服务，统一事件模型
- **分片集群增量**：mongos 拉全量 + ChangeStream@mongos（MongoDB 3.6+）

## 能力边界（不做什么）

| 不做 | 说明 / 替代 |
|------|-------------|
| 关系库（MySQL / Oracle / PostgreSQL）同步 | 用 [rds-sync](https://github.com/whaleal-dev/rds-sync) |
| Mongo ↔ 关系库异构直连 | 没有官方适配器；两个 SDK 不要串成一条链路 |
| standalone 上的增量 | standalone 无 oplog / ChangeStream 语义，仅支持 `sync.mode=FULL` |
| MongoDB ≥ 7.0 上的 Oplog | 上游已移除 oplog，启动期即校验并报错，须用 ChangeStream |
| 分片集群的多分片 OPLOG | 已下线；分片增量统一走 ChangeStream@mongos |
| 主动把 Kafka 消息落库 | Kafka 只是 Sink 形态；落库请在下游用 mongo-kafka Sink |

细节限制与已知问题见 [运行与排障 · 能力边界](guide/operations.md#能力边界与已知限制)，完整审查清单见 [ARCHITECTURE.md](ARCHITECTURE.md) §4。

## 下一步

1. [概念总览](concepts/overview.md) —— 先建立全量 / 增量 / 捕获通道 / 分桶的心智模型
2. [快速开始](getting-started/quickstart.md) —— 30 秒跑起来
3. [配置项全解](guide/configuration.md) —— 按业务填配置
