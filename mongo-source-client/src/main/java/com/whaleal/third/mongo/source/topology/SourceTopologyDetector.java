package com.whaleal.third.mongo.source.topology;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import com.whaleal.third.mongo.source.config.CaptureMode;
import com.whaleal.third.mongo.source.config.SyncMode;
import com.whaleal.third.mongo.source.oplog.MongoVersion;
import org.bson.Document;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

/**
 * 探测源端架构（standalone / replicaSet / sharding），并按能力匹配读任务。
 * <p>
 * <b>增量读能力</b>
 * <ul>
 *   <li>{@link CaptureMode#OPLOG}：可读副本集、分片集群的<b>某个 shard（mongod）</b>；
 *       <b>不可</b>读 standalone、<b>不可</b>读 mongos</li>
 *   <li>{@link CaptureMode#CHANGE_STREAM}：可读副本集、分片（mongos）；
 *       <b>不可</b>用于 standalone 增量</li>
 * </ul>
 * <b>全量读</b>：可读 standalone、副本集、sharding 的 mongos（集合扫描，与捕获协议无关）。
 */
public final class SourceTopologyDetector {

    private SourceTopologyDetector() {
    }

    /**
     * @param preferredCapture   用户配置；{@link CaptureMode#AUTO} 时按架构展开
     * @param sourceUri          源 URI（拼 shard 连接串鉴权）；可为 null
     * @param multiShardCapable  Sync=true 时可挂多 shard OPLOG；纯 Source=false
     * @param discoverShards     分片 + OPLOG 且未手配 URI 时是否 {@code listShards}
     * @param syncMode           用于校验：standalone 仅允许 FULL；FULL-only 分片不拉 shard oplog
     */
    public static SourceTopologyInfo detect(MongoClient client,
                                            CaptureMode preferredCapture,
                                            String sourceUri,
                                            boolean multiShardCapable,
                                            boolean discoverShards,
                                            SyncMode syncMode) {
        Document hello = runHello(client);
        SourceTopology topology = classify(hello);
        String setName = stringOrNull(hello, "setName");
        MongoVersion version = readVersion(client);
        SyncMode mode = syncMode == null ? SyncMode.INCREMENTAL : syncMode;

        CaptureMode requested = preferredCapture == null ? CaptureMode.AUTO : preferredCapture;
        CaptureMode resolved = resolveCaptureMode(
                requested, topology, version, multiShardCapable, mode.includesIncremental());
        validateReadCapability(topology, resolved, mode, multiShardCapable);

        List<ShardEndpoint> shards = new ArrayList<ShardEndpoint>();
        boolean multiShardOplog = false;

        // OPLOG 落在 mongos 上时：只能改写为各 shard 多源，绝不能直读 mongos
        if (resolved == CaptureMode.OPLOG && topology == SourceTopology.SHARDING) {
            multiShardOplog = true;
            if (discoverShards) {
                shards.addAll(discoverShardEndpoints(client, sourceUri));
                if (shards.isEmpty()) {
                    throw new IllegalStateException(
                            "SHARDING + OPLOG: listShards returned empty; "
                                    + "configure sourceOplogUris manually or check mongos privileges");
                }
            }
        }

        SourceTopologyInfo info = new SourceTopologyInfo(
                topology, setName, version, shards, resolved, multiShardOplog);
        System.err.println("[mongo-source] topology match: " + info.summarize()
                + " syncMode=" + mode);
        return info;
    }

    /** 兼容旧调用：默认按含增量处理。 */
    public static SourceTopologyInfo detect(MongoClient client,
                                            CaptureMode preferredCapture,
                                            String sourceUri,
                                            boolean multiShardCapable,
                                            boolean discoverShards) {
        return detect(client, preferredCapture, sourceUri, multiShardCapable, discoverShards,
                SyncMode.FULL_AND_INCREMENTAL);
    }

    /** Sync 编排：可挂多 shard OPLOG。 */
    public static SourceTopologyInfo detectForSync(MongoClient client,
                                                   CaptureMode preferredCapture,
                                                   String sourceUri,
                                                   boolean discoverShards,
                                                   SyncMode syncMode) {
        return detect(client, preferredCapture, sourceUri, true, discoverShards, syncMode);
    }

    /** 单 Source：不能挂多 shard；分片 AUTO/OPLOG 无多源能力时走 ChangeStream@mongos。 */
    public static SourceTopologyInfo detectForSource(MongoClient client,
                                                     CaptureMode preferredCapture,
                                                     String sourceUri,
                                                     SyncMode syncMode) {
        return detect(client, preferredCapture, sourceUri, false, false, syncMode);
    }

    public static SourceTopologyInfo detectForSource(MongoClient client,
                                                     CaptureMode preferredCapture,
                                                     String sourceUri) {
        return detectForSource(client, preferredCapture, sourceUri, SyncMode.FULL_AND_INCREMENTAL);
    }

    /**
     * 校验捕获方式与拓扑是否匹配用户约定的读能力。
     */
    static void validateReadCapability(SourceTopology topology,
                                       CaptureMode capture,
                                       SyncMode syncMode,
                                       boolean multiShardCapable) {
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
            if (topology == SourceTopology.SHARDING && !multiShardCapable) {
                throw new IllegalStateException(
                        "OPLOG cannot read mongos; connect shard mongod or use MongoSyncClient "
                                + "(multi-shard OPLOG) / captureMode=CHANGE_STREAM");
            }
            // SHARDING + multiShardCapable：由 Sync 改写为各 shard OPLOG，合法
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
                                          boolean multiShardCapable,
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
        switch (topology) {
            case SHARDING:
                // 分片增量：OPLOG 只能打在各 shard；有多源能力则 OPLOG，否则 ChangeStream@mongos
                return multiShardCapable ? CaptureMode.OPLOG : CaptureMode.CHANGE_STREAM;
            case REPLICA_SET:
                return CaptureMode.CHANGE_STREAM;
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

    /**
     * 从 mongos {@code listShards}（回退 {@code config.shards}）发现各 shard，并拼连接串。
     */
    public static List<ShardEndpoint> discoverShardEndpoints(MongoClient mongos, String sourceUri) {
        List<Document> shardDocs = listShardDocs(mongos);
        AuthBits auth = AuthBits.fromUri(sourceUri);
        List<ShardEndpoint> out = new ArrayList<ShardEndpoint>();
        for (Document doc : shardDocs) {
            String id = stringOrNull(doc, "_id");
            String host = stringOrNull(doc, "host");
            if (host == null || host.trim().isEmpty()) {
                continue;
            }
            ShardHost parsed = ShardHost.parse(host.trim());
            String uri = auth.buildUri(parsed);
            String shardId = id != null ? id : ("shard" + out.size());
            out.add(new ShardEndpoint(shardId, uri, parsed.replicaSet));
            System.err.println("[mongo-source] discovered shard " + shardId + " → " + maskUri(uri));
        }
        return out;
    }

    private static List<Document> listShardDocs(MongoClient mongos) {
        List<Document> out = new ArrayList<Document>();
        try {
            Document result = mongos.getDatabase("admin").runCommand(new Document("listShards", 1));
            Object shards = result.get("shards");
            if (shards instanceof List) {
                for (Object o : (List<?>) shards) {
                    if (o instanceof Document) {
                        out.add((Document) o);
                    }
                }
            }
            if (!out.isEmpty()) {
                return out;
            }
        } catch (Exception e) {
            System.err.println("[mongo-source] listShards failed, fallback config.shards: " + e.getMessage());
        }
        try {
            MongoCursor<Document> cursor = mongos.getDatabase("config")
                    .getCollection("shards")
                    .find()
                    .iterator();
            try {
                while (cursor.hasNext()) {
                    out.add(cursor.next());
                }
            } finally {
                cursor.close();
            }
        } catch (Exception e) {
            System.err.println("[mongo-source] config.shards read failed: " + e.getMessage());
        }
        return out;
    }

    private static String stringOrNull(Document doc, String key) {
        if (doc == null || !doc.containsKey(key) || doc.get(key) == null) {
            return null;
        }
        return String.valueOf(doc.get(key));
    }

    private static String maskUri(String uri) {
        if (uri == null) {
            return null;
        }
        return uri.replaceAll("://([^:/@]+):([^@]+)@", "://$1:***@");
    }

    /** {@code rs0/host1:27017,host2:27017} 或 {@code host:27017} */
    static final class ShardHost {
        final String replicaSet;
        final String hosts;

        ShardHost(String replicaSet, String hosts) {
            this.replicaSet = replicaSet;
            this.hosts = hosts;
        }

        static ShardHost parse(String hostField) {
            int slash = hostField.indexOf('/');
            if (slash > 0 && slash < hostField.length() - 1) {
                return new ShardHost(hostField.substring(0, slash), hostField.substring(slash + 1));
            }
            return new ShardHost(null, hostField);
        }
    }

    static final class AuthBits {
        final String user;
        final String password;
        final String authSource;
        final String authMechanism;

        AuthBits(String user, String password, String authSource, String authMechanism) {
            this.user = user;
            this.password = password;
            this.authSource = authSource;
            this.authMechanism = authMechanism;
        }

        static AuthBits fromUri(String sourceUri) {
            if (sourceUri == null || sourceUri.trim().isEmpty()) {
                return new AuthBits(null, null, null, null);
            }
            try {
                ConnectionString cs = new ConnectionString(sourceUri.trim());
                String user = cs.getUsername();
                char[] pwd = cs.getPassword();
                String password = pwd == null ? null : new String(pwd);
                String authSource = null;
                String mechanism = null;
                if (cs.getCredential() != null) {
                    authSource = cs.getCredential().getSource();
                    mechanism = cs.getCredential().getMechanism();
                }
                return new AuthBits(user, password, authSource, mechanism);
            } catch (Exception e) {
                System.err.println("[mongo-source] parse sourceUri for shard auth failed: " + e.getMessage());
                return new AuthBits(null, null, null, null);
            }
        }

        String buildUri(ShardHost shard) {
            StringBuilder sb = new StringBuilder("mongodb://");
            if (user != null && !user.isEmpty()) {
                sb.append(urlEncode(user));
                if (password != null) {
                    sb.append(':').append(urlEncode(password));
                }
                sb.append('@');
            }
            sb.append(shard.hosts);
            sb.append("/?");
            boolean first = true;
            if (shard.replicaSet != null && !shard.replicaSet.isEmpty()) {
                sb.append("replicaSet=").append(urlEncode(shard.replicaSet));
                first = false;
            }
            if (authSource != null && !authSource.isEmpty()) {
                if (!first) {
                    sb.append('&');
                }
                sb.append("authSource=").append(urlEncode(authSource));
                first = false;
            }
            if (authMechanism != null && !authMechanism.isEmpty()) {
                if (!first) {
                    sb.append('&');
                }
                sb.append("authMechanism=").append(urlEncode(authMechanism));
            }
            String uri = sb.toString();
            if (uri.endsWith("/?")) {
                return uri.substring(0, uri.length() - 2);
            }
            if (uri.endsWith("?")) {
                return uri.substring(0, uri.length() - 1);
            }
            return uri;
        }

        private static String urlEncode(String s) {
            try {
                return URLEncoder.encode(s, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                return s;
            }
        }
    }
}
