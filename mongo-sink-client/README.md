# mongo-sink-client

Sink 只依赖 `mongo-transfer-model`，**只识别** `TransferEvent` / `DdlEvent`。

不关心上游是 Oplog 还是 ChangeStream。

## 联用

```java
MongoSinkClient sink = MongoSinkClient.builder()
        .uri("mongodb://127.0.0.1:27017")
        .database("target_db")
        .collection("target_coll")
        .writeMode(WriteMode.STRICT)          // insert 用 InsertOne
        .onConflict(OnConflict.SKIP)         // 主键冲突：SKIP | UPSERT | FAIL
        .build();

MongoSourceClient source = MongoSourceClient.builder()
        .uri("mongodb://127.0.0.1:27017/?replicaSet=rs0")
        .database("src_db")
        .collection("src_coll")
        .listener(sink::write)
        .ddlListener(sink::applyDdl)
        .build();

source.start();
```

## 主键 / 唯一键冲突（`onConflict`）

| 值 | 行为 |
|----|------|
| `FAIL`（默认） | 冲突抛错 |
| `SKIP` | 跳过该条，stderr 记日志，继续后续 |
| `UPSERT` | insert 按 `_id` 转为 ReplaceOne upsert；无法转换则跳过并记日志 |

与 `writeMode(UPSERT)` 区别：`writeMode` 主动用 upsert 写；`onConflict(UPSERT)` 可在 `STRICT` 下遇冲突再转 upsert。

## Maven

```xml
<dependency>
  <groupId>com.whaleal.third</groupId>
  <artifactId>mongo-sink-client</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```
