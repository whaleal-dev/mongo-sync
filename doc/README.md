<p align="center">
  <img src="assets/banner.svg" alt="mongo-sync — MongoDB 文档同步 SDK" />
</p>

# mongo-sync 使用手册

> **MongoDB 文档同步 SDK / 工具（Java 8+）** —— 一套 API / 一条命令，完成全量 + 增量、DDL 跟随、多库表过滤与数据校验。

本目录是面向**接入方与运维**的完整使用手册。仓库根目录的 [README.md](../README.md) 只做产品概览与选型，深入细节以本手册为准。

---

## 按角色选择阅读路径

| 我是谁 | 建议顺序 |
|--------|----------|
| 第一次接触，想先跑起来 | [前言](intro.md) → [快速开始](getting-started/quickstart.md) → [配置项全解](guide/configuration.md) 的「最小可用配置」 |
| 要评估能不能用于我的场景 | [前言](intro.md) → [概念总览](concepts/overview.md) → [同步模式](guide/sync-modes.md) → [运行与排障](guide/operations.md) 的「能力边界」 |
| 要写代码嵌入到自己的服务 | [概念总览](concepts/overview.md) → [SDK 与控制面](guide/api.md) → 各模块 README |
| 要负责上线与运维 | [快速开始](getting-started/quickstart.md) → [配置项全解](guide/configuration.md) → [运行与排障](guide/operations.md) → [数据校验](guide/verify.md) |

---

## 章节

| 章节 | 说明 |
|------|------|
| [前言](intro.md) | 为什么做、解决什么问题、适用场景、能力边界 |
| [概念总览](concepts/overview.md) | 全量与增量、捕获通道、拓扑自适应、分桶有序写、位点、状态机 |
| [快速开始](getting-started/quickstart.md) | 脚本启动、Maven 启动、嵌入式 SDK、打包发布、数据校验 |
| [配置项全解](guide/configuration.md) | 全部配置键、默认值、必填规则与启动校验 |
| [同步模式](guide/sync-modes.md) | 四种模式的行为、全量与增量如何衔接、怎么选 |
| [SDK 与控制面](guide/api.md) | `MongoSyncClient` / `MongoMultiSyncClient`、HTTP 控制面、错误码 |
| [数据校验](guide/verify.md) | `VerifyMain`：COUNT / ID / FULL 三种比对与结果解读 |
| [运行与排障](guide/operations.md) | 部署、进度字段、告警、故障排查、已知限制、FAQ |

---

## 仓库内其他文档

| 文档 | 说明 |
|------|------|
| [../README.md](../README.md) | 产品概览、与 rds-sync 的选型对照 |
| [ARCHITECTURE.md](ARCHITECTURE.md) | 架构与功能审查：模块图、能力清单、漏洞与限制清单 |
| [bin/README.md](../bin/README.md) | `mongosync.sh` / `verify.sh` / `package.sh` 脚本入口详解 |
| [examples/](examples/) | 同步 / Kafka Sink / 校验 配置示例（`properties`） |
| [oplog/](oplog/) | 各 MongoDB 版本 oplog 样例，供解析对照 |
| [../mongo-transfer-model/README.md](../mongo-transfer-model/README.md) | 传输模型（`TransferEvent` / `DdlEvent`） |
| [../mongo-source-client/README.md](../mongo-source-client/README.md) | Source API、捕获模式、位点 SPI |
| [../mongo-sink-client/README.md](../mongo-sink-client/README.md) | MongoDB Sink API、冲突策略 |
| [../mongo-kafka-sink-client/README.md](../mongo-kafka-sink-client/README.md) | Kafka Sink 与 Change Stream 消息格式 |
| [../mongo-sync-client/README.md](../mongo-sync-client/README.md) | 同步编排、分桶、全量与增量衔接 |

---

## 与 rds-sync 的分工

两者是**姊妹产品**，控制面（`start` / `pauseIncremental` / `progress` / `canCommit` / `commit`）同构，数据面互不替代：

| | mongo-sync（本仓） | [rds-sync](https://github.com/whaleal-dev/rds-sync) |
|--|--------------------|---------------------------------------------------|
| 源 | MongoDB（Oplog / ChangeStream） | MySQL / Oracle / PostgreSQL |
| Sink | MongoDB、DocumentDB / DDS、Kafka（Change Stream） | MySQL JDBC、Kafka（行级 envelope） |
| 事件契约 | `TransferEvent` / `DdlEvent` | `RowChange` / `DdlEvent` |

关系库（含投递 Kafka）请用 rds-sync。**没有**官方 Mongo ↔ 关系库异构直连适配器，不要把两个 SDK 串成一条链路。

---

## 关于文档发布

本手册为纯 Markdown，链接均为仓库内相对路径，可直接被文档站或 GitHub Pages 消费。

- 若用 GitHub Pages 的「Deploy from a branch → `/docs`」，本仓文档目录名为 `doc/`（历史命名），需改用 Actions 工作流指定 `doc/`，或将本目录平移到 `docs/`。
- 若用 Jekyll 之类的静态站生成器，注意 `.md` 相对链接会被渲染成 `.html`，发布前需要做一次链接重写（Docusaurus 可直接消费 `.md` 链接）。

---

返回仓库首页：[README](../README.md) · QQ 交流群：**983986505**
