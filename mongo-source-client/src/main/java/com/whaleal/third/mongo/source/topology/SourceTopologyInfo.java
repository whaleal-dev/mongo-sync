package com.whaleal.third.mongo.source.topology;

import com.whaleal.third.mongo.source.config.CaptureMode;
import com.whaleal.third.mongo.source.oplog.MongoVersion;

/**
 * 源端拓扑探测结果，以及据此解析出的读任务匹配建议。
 */
public final class SourceTopologyInfo {

    private final SourceTopology topology;
    private final String setName;
    private final MongoVersion version;
    private final CaptureMode resolvedCaptureMode;

    public SourceTopologyInfo(SourceTopology topology,
                              String setName,
                              MongoVersion version,
                              CaptureMode resolvedCaptureMode) {
        this.topology = topology;
        this.setName = setName;
        this.version = version;
        this.resolvedCaptureMode = resolvedCaptureMode;
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

    /** 解析后的捕获模式（AUTO 已被展开）。 */
    public CaptureMode getResolvedCaptureMode() {
        return resolvedCaptureMode;
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
        return sb.toString();
    }
}
