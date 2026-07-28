# mongo-transfer-model

通用 MongoDB **数据传输模型**（与捕获协议无关）。

- Source（Oplog / ChangeStream / 未来其它协议）产出本模型
- Sink **只识别**本模型，不感知上游实现

## 核心类型

| 类型 | 说明 |
|------|------|
| `TransferEvent` | 文档事件：`op` / `before` / `after` / `source` / `tsMs` |
| `TransferSource` | 来源元信息：db / collection / clusterTime |
| `DdlEvent` | DDL：type + command + database/collection |
| `DdlType` | CREATE/DROP_COLLECTION、DROP_DATABASE、CREATE/DROP_INDEXES、RENAME |
| `TransferEventListener` | CRUD 回调 |
| `DdlEventListener` | DDL 回调 |

## op 约定

| op | after | before |
|----|-------|--------|
| `c` / `r` | 全量文档 | - |
| `u` | 全量文档或 `$set/$unset` | documentKey / preImage |
| `d` | - | documentKey / preImage |

## Maven

```xml
<dependency>
  <groupId>com.whaleal.third</groupId>
  <artifactId>mongo-transfer-model</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```
