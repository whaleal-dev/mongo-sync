package com.whaleal.third.mongo.source.topology;

import com.mongodb.client.MongoClient;
import com.whaleal.third.mongo.source.config.CaptureMode;
import com.whaleal.third.mongo.source.config.SyncMode;
import com.whaleal.third.mongo.source.oplog.MongoVersion;
import org.bson.Document;

/**
 * 探测源端架构（standalone / replicaSet / sharding），并按能力匹配读任务。
 * <p>
 * <b>增量读能力</b>
 * <ul>
 *   <li>{@link CaptureMode#OPLOG}：只读单个副本集（含独立部署的 shard 副本集）；
 *       <b>不可</b>读 standalone、<b>不可</b>读 mongos</li>
 *   <li>{@link CaptureMode#CHANGE_STREAM}：可读副本集、分片（mongos），要求 MongoDB 3.6+；
 *       <b>不可</b>用于 standalone 增量</li>
 * </ul>
 * <b>全量读</b>：可读 standalone、副本集、sharding 的 mongos（集合扫描，与捕获协议无关）。
 * <p>
 * <b>分片集群增量只走 ChangeStream@mongos</b>：mongos 已完成跨分片归并、全局定序与 DDL 去重。
 * 各 shard 并行读 OPLOG 没有全局序，DDL 会被重复执行，因此不再提供。分片源若低于 3.6，
 * 只能做全量（{@code syncMode=FULL}），或对每个 shard 的副本集单独建任务。
 */
public final class SourceTopologyDetector {

    private SourceTopologyDetector() {
    }

    /**
     * @param preferredCapture 用户配置；{@link CaptureMode#AUTO} 时按架构展开
     * @param sourceUri        源 URI；可为 null（保留参数以便日志与后续扩展）
     * @param syncMode         用于校验：standalone 仅允许 FULL
     */
    public static SourceTopologyInfo detect(MongoClient client,
                                            CaptureMode preferredCapture,
                                            String sourceUri,
                                            SyncMode syncMode) {
        Document hello = runHello(client);
        SourceTopology topology = classify(hello);
        String setName = stringOrNull(hello, "setName");
        MongoVersion version = readVersion(client);
        SyncMode mode = syncMode == null ? SyncMode.INCREMENTAL : syncMode;

        CaptureMode requested = preferredCapture == null ? CaptureMode.AUTO : preferredCapture;
        CaptureMode resolved = resolveCaptureMode(
                requested, topology, version, mode.includesIncremental());
        validateReadCapability(topology, resolved, mode);

        SourceTopologyInfo info = new SourceTopologyInfo(topology, setName, version, resolved);
        System.err.println("[mongo-source] topology match: " + info.summarize()
                + " syncMode=" + mode);
        return info;
    }

    public static SourceTopologyInfo detect(MongoClient client,
                                            CaptureMode preferredCapture,
                                            String sourceUri) {
        return detect(client, preferredCapture, sourceUri, SyncMode.FULL_AND_INCREMENTAL);
    }

    public static SourceTopologyInfo detectForSource(MongoClient client,
                                                     CaptureMode preferredCapture,
                                                     String sourceUri,
                                                     SyncMode syncMode) {
        return detect(client, preferredCapture, sourceUri, syncMode);
    }

    public static SourceTopologyInfo detectForSource(MongoClient client,
                                                     CaptureMode preferredCapture,
                                                     String sourceUri) {
        return detect(client, preferredCapture, sourceUri, SyncMode.FULL_AND_INCREMENTAL);
    }

    /**
     * 校验捕获方式与拓扑是否匹配读能力。
     */
    static void validateReadCapability(SourceTopology topology,
                                       CaptureMode capture,
                                       SyncMode syncMode) {
        if (capture == CaptureMode.AUTO) {
            return;
        }
        boolean needInc = syncMode != null && syncMode.includesIncremental();

        if (capture == CaptureMode.OPLOG) {
            if (topology == SourceTopology.STANDALONE) {
                throw new IllegalStateException(
                        "OPLOG cannot read STANDALONE (no local.oplog.rs); "
                                + "full-only is OK without OPLOG, or use a replica set");
            }
            if (topology == SourceTopology.SHARDING) {
                throw new IllegalStateException(
                        "OPLOG cannot read mongos, and multi-shard OPLOG is no longer supported; "
                                + "use captureMode=CHANGE_STREAM (requires MongoDB 3.6+), "
                                + "or point sourceUri at a single shard's replica set");
            }
            // REPLICA_SET：合法
            return;
        }

        if (capture == CaptureMode.CHANGE_STREAM) {
            if (topology == SourceTopology.STANDALONE && needInc) {
                throw new IllegalStateException(
                        "CHANGE_STREAM cannot do incremental on STANDALONE; "
                                + "use syncMode=FULL only, or promote to replicaSet / sharding");
            }
            // RS / SHARDING(mongos)：合法；standalone + FULL：仅快照，合法
            return;
        }
    }

    static SourceTopology classify(Document hello) {
        if (hello == null) {
            return SourceTopology.STANDALONE;
        }
        String msg = stringOrNull(hello, "msg");
        if ("isdbgrid".equalsIgnoreCase(msg)) {
            return SourceTopology.SHARDING;
        }
        if (hello.containsKey("setName") && hello.get("setName") != null) {
            return SourceTopology.REPLICA_SET;
        }
        return SourceTopology.STANDALONE;
    }

    static CaptureMode resolveCaptureMode(CaptureMode requested,
                                          SourceTopology topology,
                                          MongoVersion version,
                                          boolean needIncremental) {
        if (requested != CaptureMode.AUTO) {
            if (requested == CaptureMode.OPLOG && version != null && !version.supportsOplog()) {
                throw new IllegalStateException(
                        "MongoDB " + version.getRaw()
                                + " does not support OPLOG; use CHANGE_STREAM or captureMode=AUTO");
            }
            return requested;
        }

        // AUTO：先看版本与是否需要增量
        if (!needIncremental) {
            // 仅全量：任意拓扑都走集合扫描；用 CHANGE_STREAM Listener 承载 FULL（不启增量）
            return CaptureMode.CHANGE_STREAM;
        }
        if (version != null && !version.supportsOplog()) {
            // ≥7 强制 CS；standalone 增量会在 validate 失败
            return CaptureMode.CHANGE_STREAM;
        }
        boolean changeStreamAvailable = version == null || version.supportsChangeStream();
        switch (topology) {
            case SHARDING:
                // 只走 ChangeStream@mongos：mongos 已完成跨分片归并、全局定序与 DDL 去重。
                // 各 shard 独立读 OPLOG 没有全局序，DDL 会被重复执行，故不再支持。
                if (changeStreamAvailable) {
                    return CaptureMode.CHANGE_STREAM;
                }
                throw new IllegalStateException(
                        "SHARDING + MongoDB " + version.getRaw()
                                + ": incremental requires ChangeStream (MongoDB 3.6+); "
                                + "OPLOG cannot read mongos. Use syncMode=FULL, "
                                + "or upgrade the source, or sync each shard's replica set separately");
            case REPLICA_SET:
                // 3.6 以下没有 ChangeStream，回落到副本集 OPLOG（单源，无归并问题）
                return changeStreamAvailable ? CaptureMode.CHANGE_STREAM : CaptureMode.OPLOG;
            case STANDALONE:
            default:
                // 增量不可用；返回 CS 后由 validate 抛错（若 needIncremental）
                return CaptureMode.CHANGE_STREAM;
        }
    }

    private static Document runHello(MongoClient client) {
        try {
            return client.getDatabase("admin").runCommand(new Document("hello", 1));
        } catch (Exception e) {
            try {
                return client.getDatabase("admin").runCommand(new Document("isMaster", 1));
            } catch (Exception e2) {
                System.err.println("[mongo-source] hello/isMaster failed: " + e2.getMessage());
                return new Document();
            }
        }
    }

    private static MongoVersion readVersion(MongoClient client) {
        try {
            Document buildInfo = client.getDatabase("admin").runCommand(new Document("buildInfo", 1));
            Object v = buildInfo.get("version");
            if (v != null) {
                return MongoVersion.parse(String.valueOf(v));
            }
        } catch (Exception e) {
            System.err.println("[mongo-source] buildInfo failed: " + e.getMessage());
        }
        return null;
    }

    private static String stringOrNull(Document doc, String key) {
        if (doc == null || !doc.containsKey(key) || doc.get(key) == null) {
            return null;
        }
        return String.valueOf(doc.get(key));
    }
}
