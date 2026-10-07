# 同步模式

`sync.mode` 决定「全量」与「增量」如何组合。命名与语义在 mongo-sync / [rds-sync](https://github.com/whaleal-dev/rds-sync) 两边**故意对齐**，便于同一套运维习惯。

---

## 四种模式

| 枚举 | 行为 | 全量与增量 | 何时结束 |
|------|------|------------|----------|
| `FULL` | 仅全量 | 只有全量 | 全量扫完即结束 |
| `FULL_AND_INCREMENTAL` | 全量 ∥ 增量**并行**，全量结束后持续增量 | 并行 | 不自动结束（直到 `commit` / 停止） |
| `FULL_THEN_CATCH_UP` | 先全量，再追增量窗口，追平后停止 | **串行** | 追平上界后停止 |
| `INCREMENTAL` | 仅增量 | 只有增量 | 不自动结束 |

### 命名约定（容易记混，务必记住）

- **`AND`** = 全量与增量**并行**，并且增量**持续**
- **`THEN`** = **先**全量，**再**追平，**再**停

---

## 各模式详解

### `FULL` —— 仅全量

- 只做集合扫描，不消费变更日志
- **`standalone` 源端唯一可用的模式**（standalone 没有可用的增量语义）
- 适合：一次性搬迁、冷数据同步、只读副本初始化

```properties
sync.mode=FULL
```

### `FULL_AND_INCREMENTAL` —— 全量 ∥ 增量（并行，推荐主路径）

```text
时间轴 ──────────────────────────────────────────────►
        │◄──── 全量扫描（可能数小时）────►│
        │◄──────── 增量持续消费 ───────────────────────►│
                            ▲
                    全量结束：tryDrainAndFlush
```

- 增量从**全量开始之前**的位置消费（ChangeStream 用 `startAtOperationTime`），因此与全量扫描必然存在**重叠窗口**
- 重叠部分靠 `write.mode=UPSERT` 幂等覆盖，不会产生重复文档
- 全量结束后调用 `tryDrainAndFlush`（并行模式下不强求 inflight 归零），随后持续增量
- 适合：**不停服迁库 / 灾备**。绝大多数生产场景用这个

```properties
sync.mode=FULL_AND_INCREMENTAL
write.mode=UPSERT
window.warn.seconds=3600        # 全量太久时提前告警
```

> ⚠️ 并行模式的真实风险在**捕获窗口**：全量耗时越长，源端 oplog 越可能被覆盖掉锚点位点。一旦被覆盖，增量就断链了。请务必配置 `window.warn.seconds`（默认 3600 秒）并关注 `WINDOW WARN`。

### `FULL_THEN_CATCH_UP` —— 先全量，再追平，再停（串行）

- 全量与增量**串行**：先扫完全量，再开启增量，把窗口内的变更追平
- 追平的上界默认由全量结束时刻决定，也可显式指定增量结束位点（Builder `oplogEndTimestamp`）
- 追平后**停止**，不持续增量
- 适合：需要「知道确切的切换时点」的一次性迁移；对源端 oplog 压力更敏感的场景

```properties
sync.mode=FULL_THEN_CATCH_UP
```

### `INCREMENTAL` —— 仅增量

- 不做全量，直接消费变更日志
- **前提：目标端已有与源端一致的全量基线**（例如上一轮 `FULL` 已经跑完）
- 适合：备端已同步完成，现在只需要持续追增量

```properties
sync.mode=INCREMENTAL
```

> ⚠️ SDK 里 `MongoSyncConfig` 的默认值就是 `INCREMENTAL`。用 SDK 而忘记设 `syncMode`，**不会做全量**。详见 [配置项全解 · 同步模式](configuration.md#五同步模式)。

---

## 全量与增量的衔接细节

| 行为 | 说明 |
|------|------|
| 起始位点 | 增量从全量**开始前**的 oplog `ts` / `startAtOperationTime` 开始 ⇒ 与快照重叠，靠 UPSERT 兜底 |
| 全量结束回调 | `FULL_AND_INCREMENTAL` 执行 `tryDrainAndFlush`；`FULL_THEN_CATCH_UP` 在此时才开启增量 |
| 删表 / 改名 / 删库 | 增量识别到 `DROP` / `RENAME` / `DROP_DATABASE` ⇒ 该表**源端全量视为已完成**，提前结束全量阶段 |
| 索引 DDL | 收到 `CREATE_INDEXES` / `DROP_INDEXES` 后重新探测唯一索引，并相应调整 Sink 的有序写策略 |
| 改名后写入 | Sink 执行 rename 后切换写集合句柄，后续事件写入新集合 |
| 幂等 | 默认 `write.mode=UPSERT`，按 `_id` 覆盖，重复投递安全 |

---

## 怎么选

| 你的情况 | 选它 |
|----------|------|
| 不停服迁库 / 灾备 / 多活（源持续写） | `FULL_AND_INCREMENTAL` |
| 源端是 standalone | `FULL`（唯一可选） |
| 一次搬迁，要明确的结束点与切换时点 | `FULL_THEN_CATCH_UP` |
| 目标端已有全量基线，只需持续追增量 | `INCREMENTAL` |
| 只搬冷数据 / 只初始化只读副本 | `FULL` |

---

## 与源端拓扑的硬约束

| 源端拓扑 | `FULL` | `FULL_AND_INCREMENTAL` | `FULL_THEN_CATCH_UP` | `INCREMENTAL` |
|----------|--------|------------------------|----------------------|---------------|
| `STANDALONE` | ✅ | ❌ | ❌ | ❌ |
| `REPLICA_SET` | ✅ | ✅ | ✅ | ✅ |
| `SHARDING`（mongos） | ✅ | ✅（ChangeStream@mongos，需 3.6+） | ✅ | ✅ |

不满足的组态在 `AUTO` 下会在启动时报错，而不是跑到一半失败。

---

## 停止与提交

| 动作 | 效果 |
|------|------|
| Ctrl+C / `--shutdown` / `SIGTERM` | 优雅停止：停捕获、排空写入、关资源（状态转 `STOPPED`） |
| `pause()` | 全量 + 增量都停（**初始全量进行中不允许**，改用 `pauseIncremental()`） |
| `pauseIncremental()` | 只冻增量，全量继续跑 |
| `canCommit()` → `commit()` | 最小 cutover：停捕获 + 排空 + 标记 `COMMITTED` |
| `commit.when.ready=true` | 达到 `canCommit` 自动 commit 并退出 |

`canCommit` 的判定条件见 [概念总览 · 状态机与 cutover](../concepts/overview.md#8-状态机与-cutover)。

---

下一步：[SDK 与控制面](api.md) · [数据校验](verify.md)
