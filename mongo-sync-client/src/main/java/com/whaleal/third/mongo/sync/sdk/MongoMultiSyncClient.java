package com.whaleal.third.mongo.sync.sdk;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.whaleal.third.mongo.source.config.CaptureMode;
import com.whaleal.third.mongo.source.topology.ShardEndpoint;
import com.whaleal.third.mongo.source.topology.SourceTopologyDetector;
import com.whaleal.third.mongo.source.topology.SourceTopologyInfo;
import com.whaleal.third.mongo.sync.config.MongoMultiSyncConfig;
import com.whaleal.third.mongo.sync.config.MongoSyncConfig;
import com.whaleal.third.mongo.sync.ns.CollectionDiscovery;
import com.whaleal.third.mongo.sync.ns.NamespaceFilter;
import com.whaleal.third.mongo.sync.ns.NamespaceMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 多库表同步编排：按白/黑名单发现集合，为每张表启动一个 {@link MongoSyncClient}（共享源/目标 MongoClient）。
 * <p>
 * 对齐 MongoShake 库表过滤能力；位点可选文件持久化（{@link MongoMultiSyncConfig.Builder#offsetStoreDir}）。
 */
public final class MongoMultiSyncClient implements AutoCloseable {

    private final MongoMultiSyncConfig config;
    private final MongoClient sourceClient;
    private final MongoClient targetClient;
    private final boolean ownsSourceClient;
    private final boolean ownsTargetClient;
    private final List<MongoSyncClient> children;
    private final List<NamespaceMapper.NsPair> namespaces;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private MongoMultiSyncClient(MongoMultiSyncConfig config,
                                 MongoClient sourceClient,
                                 boolean ownsSourceClient,
                                 MongoClient targetClient,
                                 boolean ownsTargetClient,
                                 List<NamespaceMapper.NsPair> namespaces,
                                 List<MongoSyncClient> children) {
        this.config = config;
        this.sourceClient = sourceClient;
        this.ownsSourceClient = ownsSourceClient;
        this.targetClient = targetClient;
        this.ownsTargetClient = ownsTargetClient;
        this.namespaces = Collections.unmodifiableList(namespaces);
        this.children = Collections.unmodifiableList(children);
    }

    public static MongoMultiSyncClient create(MongoMultiSyncConfig.Builder builder) {
        return create(builder.build());
    }

    public static MongoMultiSyncClient create(MongoMultiSyncConfig config) {
        boolean ownsSource = false;
        boolean ownsTarget = false;
        MongoClient source;
        MongoClient target;
        if (config.getSourceMongoClient() != null) {
            source = config.getSourceMongoClient();
        } else {
            source = MongoClients.create(config.getSourceUri());
            ownsSource = true;
        }
        if (config.getTargetMongoClient() != null) {
            target = config.getTargetMongoClient();
        } else {
            target = MongoClients.create(config.getTargetUri());
            ownsTarget = true;
        }

        NamespaceFilter filter = config.namespaceFilter();
        if (filter.isEmpty()) {
            throw new IllegalArgumentException(
                    "namespaceWhite or namespaceBlack is required for multi-sync "
                            + "(e.g. namespaceWhite=demo;app.orders). "
                            + "For single collection use MongoSyncClient.");
        }

        List<NamespaceMapper.NsPair> pairs =
                CollectionDiscovery.discover(source, filter, config.namespaceMapper());
        if (pairs.isEmpty()) {
            if (ownsSource) {
                source.close();
            }
            if (ownsTarget) {
                target.close();
            }
            throw new IllegalStateException(
                    "no collections matched namespace filter white="
                            + config.getNamespaceWhite() + " black=" + config.getNamespaceBlack());
        }

        // 多表共享一次拓扑探测，避免每张表重复 listShards
        boolean hasExplicitShards = (config.getSourceOplogUris() != null
                && !config.getSourceOplogUris().isEmpty())
                || (config.getSourceOplogUrisSemicolon() != null
                && !config.getSourceOplogUrisSemicolon().trim().isEmpty());
        SourceTopologyInfo topologyInfo = SourceTopologyDetector.detect(
                source,
                config.getCaptureMode(),
                config.getSourceUri(),
                true,
                !hasExplicitShards,
                config.getSyncMode());
        CaptureMode resolvedCapture = topologyInfo.getResolvedCaptureMode();

        List<MongoSyncClient> children = new ArrayList<MongoSyncClient>(pairs.size());
        try {
            for (NamespaceMapper.NsPair pair : pairs) {
                MongoSyncConfig.Builder b = MongoSyncClient.builder()
                        .sourceMongoClient(source)
                        .targetMongoClient(target)
                        .closeSourceClientOnStop(false)
                        .closeTargetClientOnClose(false)
                        .sourceDatabase(pair.sourceDatabase)
                        .sourceCollection(pair.sourceCollection)
                        .targetDatabase(pair.targetDatabase)
                        .targetCollection(pair.targetCollection)
                        .captureMode(resolvedCapture)
                        .syncMode(config.getSyncMode())
                        .fullDocument(config.getFullDocument())
                        .enablePreImage(config.isEnablePreImage())
                        .includeFromMigrate(config.isIncludeFromMigrate())
                        .writeMode(config.getWriteMode())
                        .onConflict(config.getOnConflict())
                        .targetBatchSize(config.getTargetBatchSize())
                        .targetWriterThreads(config.getTargetWriterThreads())
                        .bucketNum(config.getBucketNum())
                        .bucketQueueCapacity(config.getBucketQueueCapacity())
                        .ddlWaitSeconds(config.getDdlWaitSeconds())
                        .forceSingleBucketOnUniqueIndex(config.isForceSingleBucketOnUniqueIndex())
                        .writeErrorHandler(config.getWriteErrorHandler())
                        .bootstrapCollection(config.isBootstrapCollection())
                        .bootstrapIndexes(config.isBootstrapIndexes())
                        .skipTtlIndexes(config.isSkipTtlIndexes())
                        .offsetLogIntervalSeconds(config.getOffsetLogIntervalSeconds())
                        .fullSyncParallelism(config.getFullSyncParallelism())
                        .fullSyncBatchSize(config.getFullSyncBatchSize())
                        .fullSyncTaskMbSize(config.getFullSyncTaskMbSize());

                if (config.getMongoVersion() != null) {
                    b.mongoVersion(config.getMongoVersion());
                } else if (topologyInfo.getVersion() != null) {
                    b.mongoVersion(topologyInfo.getVersion());
                }
                if (config.getOffsetStoreDir() != null && !config.getOffsetStoreDir().trim().isEmpty()) {
                    b.offsetStoreDir(config.getOffsetStoreDir().trim());
                }
                if (resolvedCapture == CaptureMode.OPLOG) {
                    if (config.getSourceOplogUris() != null && !config.getSourceOplogUris().isEmpty()) {
                        b.sourceOplogUris(config.getSourceOplogUris());
                    } else if (config.getSourceOplogUrisSemicolon() != null
                            && !config.getSourceOplogUrisSemicolon().trim().isEmpty()) {
                        b.sourceOplogUrisSemicolon(config.getSourceOplogUrisSemicolon());
                    } else if (topologyInfo.isMultiShardOplog() && !topologyInfo.getShards().isEmpty()) {
                        List<String> uris = new ArrayList<String>();
                        List<String> names = new ArrayList<String>();
                        for (ShardEndpoint ep : topologyInfo.getShards()) {
                            uris.add(ep.getUri());
                            names.add(ep.getShardId());
                        }
                        b.sourceOplogUris(uris);
                        b.sourceOplogShardNames(names.toArray(new String[names.size()]));
                    }
                    if (config.getSourceOplogShardNames() != null
                            && !config.getSourceOplogShardNames().isEmpty()) {
                        b.sourceOplogShardNames(config.getSourceOplogShardNames()
                                .toArray(new String[config.getSourceOplogShardNames().size()]));
                    }
                }
                children.add(MongoSyncClient.create(b));
            }
        } catch (RuntimeException e) {
            for (MongoSyncClient child : children) {
                try {
                    child.close();
                } catch (Exception ignored) {
                }
            }
            if (ownsSource) {
                source.close();
            }
            if (ownsTarget) {
                target.close();
            }
            throw e;
        }

        System.err.println("[mongo-sync] multi-sync discovered " + pairs.size()
                + " collection(s): " + summarize(pairs));
        return new MongoMultiSyncClient(
                config, source, ownsSource, target, ownsTarget, pairs, children);
    }

    public void start() {
        if (stopped.get()) {
            throw new IllegalStateException("multi-sync already stopped");
        }
        if (!started.compareAndSet(false, true)) {
            return;
        }
        for (MongoSyncClient child : children) {
            child.start();
        }
    }

    public void stop() {
        close();
    }

    public List<NamespaceMapper.NsPair> getNamespaces() {
        return namespaces;
    }

    public int collectionCount() {
        return children.size();
    }

    public boolean canCommit() {
        if (children.isEmpty()) {
            return false;
        }
        for (MongoSyncClient child : children) {
            if (!child.canCommit()) {
                return false;
            }
        }
        return true;
    }

    public MigrationProgress progress() {
        MigrationState state = MigrationState.IDLE;
        long snapshot = 0;
        long incremental = 0;
        long ddl = 0;
        long inflight = 0;
        long startedAt = 0;
        Long committedAt = null;
        Long lastEventTs = null;
        boolean fullComplete = true;
        int shardSources = 0;

        for (MongoSyncClient child : children) {
            MigrationProgress p = child.progress();
            state = maxState(state, p.getState());
            snapshot += p.getSnapshotEvents();
            incremental += p.getIncrementalEvents();
            ddl += p.getDdlEvents();
            inflight += p.getInflightEvents();
            shardSources += p.getShardSourceCount();
            if (!p.isFullSyncComplete()) {
                fullComplete = false;
            }
            if (startedAt == 0 || (p.getStartedAtMs() > 0 && p.getStartedAtMs() < startedAt)) {
                startedAt = p.getStartedAtMs();
            }
            if (p.getCommittedAtMs() == null) {
                committedAt = null;
            } else if (committedAt == null || p.getCommittedAtMs() > committedAt.longValue()) {
                committedAt = p.getCommittedAtMs();
            }
            if (p.getLastEventTsMs() != null
                    && (lastEventTs == null || p.getLastEventTsMs() > lastEventTs.longValue())) {
                lastEventTs = p.getLastEventTsMs();
            }
        }

        return new MigrationProgress(
                canCommit() ? MigrationState.CAN_COMMIT : state,
                canCommit(),
                fullComplete,
                snapshot,
                incremental,
                ddl,
                inflight,
                shardSources,
                lastEventTs,
                startedAt,
                committedAt,
                "collections=" + children.size());
    }

    public MigrationProgress commit() {
        if (!canCommit()) {
            throw new IllegalStateException("multi-sync not ready to commit: " + progress());
        }
        for (MongoSyncClient child : children) {
            child.commit();
        }
        return progress();
    }

    public MongoMultiSyncConfig getConfig() {
        return config;
    }

    @Override
    public void close() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        for (MongoSyncClient child : children) {
            try {
                child.close();
            } catch (Exception ignored) {
            }
        }
        if (ownsSourceClient && sourceClient != null) {
            try {
                sourceClient.close();
            } catch (Exception ignored) {
            }
        }
        if (ownsTargetClient && targetClient != null) {
            try {
                targetClient.close();
            } catch (Exception ignored) {
            }
        }
    }

    private static String summarize(List<NamespaceMapper.NsPair> pairs) {
        StringBuilder sb = new StringBuilder();
        int n = Math.min(pairs.size(), 20);
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            NamespaceMapper.NsPair p = pairs.get(i);
            sb.append(p.sourceNs());
            if (!p.sourceNs().equals(p.targetNs())) {
                sb.append("->").append(p.targetNs());
            }
        }
        if (pairs.size() > n) {
            sb.append(", ...(+").append(pairs.size() - n).append(')');
        }
        return sb.toString();
    }

    private static MigrationState maxState(MigrationState left, MigrationState right) {
        if (left == MigrationState.ERROR || right == MigrationState.ERROR) {
            return MigrationState.ERROR;
        }
        if (left == MigrationState.COMMITTING || right == MigrationState.COMMITTING) {
            return MigrationState.COMMITTING;
        }
        if (left == MigrationState.COMMITTED && right == MigrationState.COMMITTED) {
            return MigrationState.COMMITTED;
        }
        if (left == MigrationState.RUNNING || right == MigrationState.RUNNING) {
            return MigrationState.RUNNING;
        }
        if (left == MigrationState.CAN_COMMIT || right == MigrationState.CAN_COMMIT) {
            return MigrationState.CAN_COMMIT;
        }
        if (left == MigrationState.STOPPED || right == MigrationState.STOPPED) {
            return MigrationState.STOPPED;
        }
        return right == null ? left : right;
    }
}
