# 脚本入口

## 同步 / 校验（推荐，无需先 package）

```bash
cd mongo-sync
chmod +x bin/*.sh

# 同步（默认用 doc/examples/mongo-sync.example.properties）
./bin/mongosync.sh
./bin/mongosync.sh -f doc/examples/mongo-sync.example.properties
./bin/mongosync.sh --config doc/examples/mongo-sync.example.properties
CONF=/path/to.properties ./bin/mongosync.sh

# 开启 progress 日志与自动 commit
./bin/mongosync.sh --progress-log-seconds 5 --commit-when-ready -f doc/examples/mongo-sync.example.properties

# 关闭同一份配置启动的 mongosync 进程
./bin/mongosync.sh --config doc/examples/mongo-sync.example.properties --shutdown

# 校验
./bin/verify.sh
./bin/verify.sh -f doc/examples/mongo-verify.example.properties
./bin/verify.sh \
  --source-uri 'mongodb://127.0.0.1:27017/?replicaSet=rs0' \
  --target-uri 'mongodb://127.0.0.1:27018' \
  --source-db demo --source-coll orders --mode FULL
```

- 同步入口：`com.whaleal.third.mongo.sync.launcher.SyncMain`
- 校验入口：`com.whaleal.third.mongo.sync.verify.VerifyMain`
- 未打 fat jar 时脚本自动走 `mvn exec:java`
- 同步时会周期打印 `MigrationProgress`
- Ctrl+C 停止同步
- `-f` / `--config` 可显式指定配置文件
- `--shutdown` 会按配置文件定位对应的运行实例并发送 `TERM`

校验退出码：`0` 通过，`1` 有差异，`2` 错误。

## 可选：组装 dist（fat jar）

```bash
./bin/package.sh
# 之后 ./bin/mongosync.sh / ./bin/verify.sh 会优先使用 dist/lib/mongo-sync-all.jar
```

```text
dist/
├── bin/
├── conf/mongo-sync.properties
├── conf/mongo-verify.properties
└── lib/mongo-sync-all.jar
```
