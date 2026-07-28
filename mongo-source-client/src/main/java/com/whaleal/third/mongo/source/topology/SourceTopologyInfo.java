package com.whaleal.third.mongo.source.topology;

import com.whaleal.third.mongo.source.config.CaptureMode;
import com.whaleal.third.mongo.source.oplog.MongoVersion;

import java.util.Collections;
import java.util.List;

/**
 * 源端拓扑探测结果，以及据此解析出的读任务匹配建议。
 */
public final class SourceTopologyInfo {

    private final SourceTopology topology;
    private final String setName;
    private final MongoVersion version;
    private final List<ShardEndpoint> shards;
    private final CaptureMode resolvedCaptureMode;
    private final boolean multiShardOplog;

    public SourceTopologyInfo(SourceTopology topology,
                              String setName,
                              MongoVersion version,
                              List<ShardEndpoint> shards,
                              CaptureMode resolvedCaptureMode,
                              boolean multiShardOplog) {
        this.topology = topology;
        this.setName = setName;
        this.version = version;
        this.shards = shards == null
                ? Collections.<ShardEndpoint>emptyList()
                : Collections.unmodifiableList(shards);
        this.resolvedCaptureMode = resolvedCaptureMode;
        this.multiShardOplog = multiShardOplog;
    }

    public SourceTopology getTopology() {
        return topology;
    }

    public String getSetName() {
        return setName;
    }

    public MongoVersion getVersion() {
        return version;
    }

    public List<ShardEndpoint> getShards() {
        return shards;
    }

    /** 解析后的捕获模式（AUTO 已被展开）。 */
    public CaptureMode getResolvedCaptureMode() {
        return resolvedCaptureMode;
    }

    /** 是否应按各 shard 并行拉 OPLOG（全量仍走 mongos）。 */
    public boolean isMultiShardOplog() {
        return multiShardOplog;
    }

    public String summarize() {
        StringBuilder sb = new StringBuilder();
        sb.append("topology=").append(topology);
        if (setName != null) {
            sb.append(" setName=").append(setName);
        }
        if (version != null) {
            sb.append(" version=").append(version.getRaw());
        }
        sb.append(" capture=").append(resolvedCaptureMode);
        if (multiShardOplog) {
            sb.append(" shardOplog=").append(shards.size());
        }
        return sb.toString();
    }
}
