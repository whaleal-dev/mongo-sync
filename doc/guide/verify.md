# 数据校验

迁移「跑完了」不等于「数据是对的」。`VerifyMain` 提供独立于同步链路的比对能力，用于切换前的验收与日常抽检。

---

## 三种比对深度

| `verify.mode` | 比对内容 | 成本 | 用在哪 |
|---------------|----------|------|--------|
| `COUNT` | 仅文档数 | 最低 | 日常巡检、大表快速对账 |
| `ID` | `_id` 集合（含「源端有、Sink 没有」与反向） | 中 | 定位缺文档 / 多文档 |
| `FULL`（默认） | `_id` + 文档内容（可忽略字段） | 最高 | 切换前验收 |

---

## 运行方式

### 脚本（推荐）

```bash
cd mongo-sync
./bin/verify.sh -f <your-verify.properties>
```

### Maven

```bash
mvn -q -pl mongo-sync-client -am package -DskipTests exec:java \
  -Dexec.mainClass=com.whaleal.third.mongo.sync.verify.VerifyMain \
  -Dexec.args="doc/examples/mongo-verify.example.properties"
```

### 简参（单表，快速对一把）

```bash
./bin/verify.sh \
  --source-uri 'mongodb://127.0.0.1:27017/?replicaSet=rs0' \
  --sink-uri 'mongodb://127.0.0.1:27018' \
  --source-db demo --source-coll orders --mode FULL
```

命令行参数：`--source-uri`、`--sink-uri`、`--source-db`、`--source-coll`、`--sink-db`、`--sink-coll`、`--mode`、`--namespace-white`、`--config`。

---

## 配置

```properties
source.uri=mongodb://127.0.0.1:27017/?replicaSet=rs0
sink.uri=mongodb://127.0.0.1:27018

# 单表
source.database=demo
source.collection=orders
sink.database=demo
sink.collection=orders

# 或多表（配置后忽略上面的单表；与同步白名单保持一致）
# namespace.white=demo;app.orders
# namespace.black=
# namespace.transform=demo.orders:backup.orders

# 比对策略
verify.mode=FULL
verify.max.samples=50
verify.batch.size=500
verify.ignore.fields=updatedAt;_class
```

| 键 | 默认 | 说明 |
|----|------|------|
| `source.uri` / `sink.uri` | — | **必填**，两端连接串 |
| `source.database` + `source.collection` | — | 单表模式必填 |
| `sink.database` / `sink.collection` | 同源 | 目标命名空间 |
| `namespace.white` / `namespace.black` / `namespace.transform` | — | 多表模式；语义与同步侧完全一致（含 `namespace.transform`） |
| `verify.mode` | `FULL` | `COUNT` \| `ID` \| `FULL` |
| `verify.max.samples` | `50` | 差异样本打印条数上限 |
| `verify.batch.size` | `500` | 读取批大小 |
| `verify.ignore.fields` | 空 | 比对时忽略的字段，**分号分隔**，如 `updatedAt;_class` |

模板：[examples/mongo-verify.example.properties](../examples/mongo-verify.example.properties)。

> 多表模式会先按白名单**发现源端集合**，再套用 `namespace.transform` 得到目标命名空间；因此源端新增集合会被自动纳入，不需要改校验配置。

---

## 退出码与输出

| 退出码 | 含义 |
|--------|------|
| `0` | 全部通过 |
| `1` | 存在差异 |
| `2` | 参数 / 运行错误 |

可直接接 CI 或切换脚本：

```bash
if ./bin/verify.sh -f verify.properties; then
  echo "校验通过，可以切流"
else
  echo "存在差异（退出码 $?），中止切换"
fi
```

输出形如：

```text
[mongo-verify] mode=FULL collections=2
CollectionVerifyReport{demo.orders, passed=true, ...}
  - ...          ← 差异样本（最多 verify.max.samples 条）
[mongo-verify] done total=2 pass=1 fail=1
```

- 每个集合一份报告，逐条打印差异样本
- 末尾汇总 `total / pass / fail`
- 只要有 `fail`，退出码即为 `1`

---

## 实践建议

| 建议 | 原因 |
|------|------|
| 切换前跑一次 `FULL`，日常跑 `COUNT` | 成本与风险的平衡 |
| 忽略自动维护字段（`updatedAt`、`_class`、`version` 之类） | 两端写入时间不同，这类字段天然有差异 |
| 校验配置与同步白名单保持一致 | 否则会出现「漏校验」或「校验了没同步的库」 |
| 校验期间避免大流量写入，或接受少量差异 | 校验不是快照读，写入并发时会出现瞬时不一致 |
| 大表先用 `COUNT` 定范围，再对可疑集合用 `FULL` | `FULL` 需要全量读两端 |
| 与位点持久化配合使用 | 位点不持久化时崩溃重跑可能留下「重复或缺失」，需要校验兜底 |

> ⚠️ `verify.mode=ID` 报告的两类差异含义不同：**缺 Sink** 说明该文档没同步过去；**缺源** 说明 Sink 多出了文档（多为重复投递 · 或源端已删而 Sink 未收到删除事件）。定位方向完全不同，别混着看。

---

下一步：[运行与排障](operations.md)
