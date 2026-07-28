# 配置示例

- 同步：[mongo-sync.example.properties](./mongo-sync.example.properties) → `SyncMain` / `MongoSyncClient`
- 比对：[mongo-verify.example.properties](./mongo-verify.example.properties) → `VerifyMain`

## 脚本入口（推荐）

```bash
cd mongo-sync
chmod +x bin/*.sh
./bin/package.sh
./bin/mongosync.sh         # 或 ./bin/mongosync.sh path/to.properties
./bin/verify.sh            # 或 ./bin/verify.sh path/to-verify.properties
```

详见 [bin/README.md](../../bin/README.md)。

## 数据比对（Maven）

```bash
mvn -pl mongo-sync-client -am package -DskipTests
mvn -pl mongo-sync-client exec:java \
  -Dexec.mainClass=com.whaleal.third.mongo.sync.verify.VerifyMain \
  -Dexec.args="doc/examples/mongo-verify.example.properties"
```

退出码：`0` 通过，`1` 有差异，`2` 错误。

## 同步（Maven）

```bash
mvn -pl mongo-sync-client exec:java \
  -Dexec.mainClass=com.whaleal.third.mongo.sync.launcher.SyncMain \
  -Dexec.args="doc/examples/mongo-sync.example.properties"
```

## 同步最小用法

```java
Properties p = new Properties();
try (InputStream in = Files.newInputStream(Paths.get("mongo-sync.example.properties"));
     Reader r = new InputStreamReader(in, StandardCharsets.UTF_8)) {
    p.load(r);
}

MongoSyncClient sync = MongoSyncClient.create(MongoSyncClient.builder()
        .sourceUri(p.getProperty("source.uri"))
        .targetUri(p.getProperty("target.uri"))
        .sourceDatabase(p.getProperty("source.database"))
        .sourceCollection(p.getProperty("source.collection"))
        .targetDatabase(p.getProperty("target.database"))
        .targetCollection(p.getProperty("target.collection"))
        .captureMode(CaptureMode.valueOf(p.getProperty("capture.mode", "CHANGE_STREAM")))
        .syncMode(SyncMode.valueOf(p.getProperty("sync.mode", "INCREMENTAL")))
        .bootstrapCollection(Boolean.parseBoolean(p.getProperty("bootstrap.collection", "true")))
        .bootstrapIndexes(Boolean.parseBoolean(p.getProperty("bootstrap.indexes", "true")))
        .offsetLogIntervalSeconds(Integer.parseInt(p.getProperty("offset.log.interval.seconds", "30")))
        .writeErrorHandler((bucket, event, err) -> {
            // TODO
        }));
sync.start();
```

## Oplog 模式片段

```properties
capture.mode=OPLOG
mongo.version=4.4.29
sync.mode=FULL_AND_INCREMENTAL
```

## 仅增量、不建索引

```properties
sync.mode=INCREMENTAL
bootstrap.collection=true
bootstrap.indexes=false
```
