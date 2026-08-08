package com.whaleal.third.mongo.source.config;

/**
 * CDC 增量捕获模式。
 */
public enum CaptureMode {

    /**
     * 按源端架构 + 版本 + SyncMode 自动匹配（见 {@link com.whaleal.third.mongo.source.topology.SourceTopologyDetector}）。
     * <ul>
     *   <li>全量：standalone / 副本集 / mongos 均可集合扫描</li>
     *   <li>副本集增量：≥3.6 优先 ChangeStream，更低版本回落 OPLOG</li>
     *   <li>分片增量：仅 ChangeStream@mongos（≥3.6）；不再提供多分片 OPLOG</li>
     *   <li>standalone：仅支持全量</li>
     * </ul>
     */
    AUTO,

    /**
     * ChangeStream：可读副本集、分片 mongos；不可用于 standalone 增量。
     */
    CHANGE_STREAM,

    /**
     * {@code local.oplog.rs}：仅可读单个副本集（含独立部署的 shard 副本集）；
     * 不可读 standalone / mongos。分片集群请用 {@link #CHANGE_STREAM}。MongoDB ≥7.0 禁止。
     */
    OPLOG
}
