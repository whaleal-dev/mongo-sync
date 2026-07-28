package com.whaleal.third.mongo.source.config;

/**
 * CDC 增量捕获模式。
 */
public enum CaptureMode {

    /**
     * 按源端架构 + 版本 + SyncMode 自动匹配（见 {@link com.whaleal.third.mongo.source.topology.SourceTopologyDetector}）。
     * <ul>
     *   <li>全量：standalone / 副本集 / mongos 均可集合扫描</li>
     *   <li>增量 OPLOG：仅副本集或各 shard mongod（禁止 standalone / mongos）</li>
     *   <li>增量 ChangeStream：副本集或 mongos（禁止 standalone 增量）</li>
     * </ul>
     */
    AUTO,

    /**
     * ChangeStream：可读副本集、分片 mongos；不可用于 standalone 增量。
     */
    CHANGE_STREAM,

    /**
     * {@code local.oplog.rs}：可读副本集、分片的某个 shard；不可读 standalone / mongos。
     * 分片场景由 Sync 自动 {@code listShards} 多源拉取。MongoDB ≥7.0 禁止。
     */
    OPLOG
}
