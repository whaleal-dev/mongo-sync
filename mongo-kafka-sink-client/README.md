# mongo-kafka-sink-client

mongo-sync 的 **Kafka 目标**实现：把 `TransferEvent` / `DdlEvent` 写成
[mongo-kafka](https://www.mongodb.com/docs/kafka-connector/current/) Source 兼容的 **Change Stream** 消息。

下游可用 mongo-kafka Sink 的 `ChangeStreamHandler` 再写入另一套 MongoDB，或由业务直接消费。

```text
MongoSourceClient → TransferEvent → KafkaSinkClient → Kafka topic
```

## 消息格式

| Kafka | 内容 |
|-------|------|
| key | `documentKey` 的 STRICT JSON（同 mongo-kafka `output.format.key=json`） |
| value（默认 JSON） | Change Stream 文档：`operationType` / `ns` / `documentKey` / `fullDocument` / `updateDescription` |
| value（BSON） | 同一文档的 BSON 字节 |

操作映射：`c`/`r` → `insert`；全量文档 `u` → `replace`；`$set/$unset` 的 `u` → `update`；`d` → `delete`。

Topic 默认 `{prefix}{sep}{db}{sep}{coll}{sep}{suffix}`，与 mongo-kafka `DefaultTopicMapper` 一致。

## SDK

```java
KafkaSinkClient sink = KafkaSinkClient.builder()
        .bootstrapServers("127.0.0.1:9092")
        .database("demo")
        .collection("orders")
        .topicPrefix("mongo")
        .build();
sink.write(event);
sink.flushAndWait();
```

通常不必直接使用本模块，由 `MongoSyncClient` 在 `targetType=KAFKA` 时装配。
