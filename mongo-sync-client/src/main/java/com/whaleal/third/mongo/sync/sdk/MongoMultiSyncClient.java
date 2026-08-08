package com.whaleal.third.mongo.sync.sdk;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.whaleal.third.mongo.source.config.CaptureMode;
import com.whaleal.third.mongo.source.topology.SourceTopologyDetector;
import com.whaleal.third.mongo.source.topology.SourceTopologyInfo;
import com.whaleal.third.mongo.sync.config.MongoMultiSyncConfig;
import com.whaleal.third.mongo.sync.config.MongoSyncConfig;
import com.whaleal.third.mongo.sync.error.MongoSyncErrorCode;
import com.whaleal.third.mongo.sync.error.MongoSyncException;
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

        // 多表共享一次拓扑探测，避免每张表重复探测
        SourceTopologyInfo topologyInfo = SourceTopologyDetector.detect(
                source,
                config.getCaptureMode(),
                config.getSourceUri(),
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
                        .fullSyncTaskMbSize(config.getFullSyncTaskMbSize())
                        .commitMaxLagMs(config.getCommitMaxLagMs());

                if (config.getMongoVersion() != null) {
                    b.mongoVersion(config.getMongoVersion());
                } else if (topologyInfo.getVersion() != null) {
                    b.mongoVersion(topologyInfo.getVersion());
                }
                if (config.getOffsetStoreDir() != null && !config.getOffsetStoreDir().trim().isEmpty()) {
                    b.offsetStoreDir(config.getOffsetStoreDir().trim());
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
            throw new MongoSyncException(MongoSyncErrorCode.CLIENT_STATE_INVALID,
                    "multi-sync already stopped");
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

    public synchronized MigrationProgress pause() {
        List<ChildFailure> failures = applyToAllChildren(new ChildTask() {
            @Override
            public void apply(MongoSyncClient child) {
                child.pause();
            }
        });
        MigrationProgress p = progress();
        throwIfPartialFailures("pause", failures, p);
        return p;
    }

    public void resume() {
        for (MongoSyncClient child : children) {
            child.start();
        }
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
        long estimatedTotal = 0;
        long startedAt = 0;
        Long maxCommittedAt = null;
        boolean allCommitted = !children.isEmpty();
        Long lastEventTs = null;
        Long lagMs = null;
        boolean fullComplete = true;

        for (MongoSyncClient child : children) {
            MigrationProgress p = child.progress();
            state = maxState(state, p.getState());
            snapshot += p.getSnapshotEvents();
            incremental += p.getIncrementalEvents();
            ddl += p.getDdlEvents();
            inflight += p.getInflightEvents();
            estimatedTotal += p.getEstimatedTotalDocuments();
            if (!p.isFullSyncComplete()) {
                fullComplete = false;
            }
            if (startedAt == 0 || (p.getStartedAtMs() > 0 && p.getStartedAtMs() < startedAt)) {
                startedAt = p.getStartedAtMs();
            }
            if (p.getCommittedAtMs() == null) {
                allCommitted = false;
            } else if (maxCommittedAt == null || p.getCommittedAtMs() > maxCommittedAt.longValue()) {
                maxCommittedAt = p.getCommittedAtMs();
            }
            if (p.getLastEventTsMs() != null
                    && (lastEventTs == null || p.getLastEventTsMs() > lastEventTs.longValue())) {
                lastEventTs = p.getLastEventTsMs();
            }
            if (p.getLagMs() != null && (lagMs == null || p.getLagMs() > lagMs.longValue())) {
                lagMs = p.getLagMs();
            }
        }
        Long committedAt = allCommitted ? maxCommittedAt : null;

        return new MigrationProgress(
                "multi(" + children.size() + ")",
                phaseOf(canCommit() ? MigrationState.CAN_COMMIT : state, fullComplete),
                "MIXED",
                "AUTO",
                config.getSyncMode() == null ? "UNKNOWN" : config.getSyncMode().name(),
                canCommit() ? MigrationState.CAN_COMMIT : state,
                canCommit(),
                fullComplete,
                estimatedTotal,
                snapshot,
                incremental,
                ddl,
                inflight,
                lastEventTs,
                startedAt,
                committedAt,
                startedAt > 0 ? (System.currentTimeMillis() - startedAt) : 0,
                lagMs,
                children.size(),
                "collections=" + children.size(),
                canCommit() ? "ready" : "waiting child migrations");
    }

    public synchronized MigrationProgress commit() {
        List<ChildFailure> failures = applyToAllChildren(new ChildTask() {
            @Override
            public void apply(MongoSyncClient child) {
                child.commit();
            }
        });
        MigrationProgress p = progress();
        throwIfPartialFailures("commit", failures, p);
        return p;
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

    private interface ChildTask {
        void apply(MongoSyncClient child);
    }

    private static final class ChildFailure {
        private final String namespace;
        private final RuntimeException error;

        private ChildFailure(String namespace, RuntimeException error) {
            this.namespace = namespace;
            this.error = error;
        }

        private String describe() {
            if (error instanceof MongoSyncException) {
                MongoSyncException mse = (MongoSyncException) error;
                return namespace + ": [" + mse.getCode() + "] " + mse.getMessage();
            }
            return namespace + ": " + error.getMessage();
        }
    }

    private List<ChildFailure> applyToAllChildren(ChildTask task) {
        List<ChildFailure> failures = new ArrayList<ChildFailure>();
        for (MongoSyncClient child : children) {
            try {
                task.apply(child);
            } catch (RuntimeException e) {
                failures.add(new ChildFailure(child.getConfig().sourceNs(), e));
            }
        }
        return failures;
    }

    private void throwIfPartialFailures(String operation,
                                        List<ChildFailure> failures,
                                        MigrationProgress progressSnapshot) {
        if (failures.isEmpty()) {
            return;
        }
        StringBuilder msg = new StringBuilder();
        msg.append("multi-sync ").append(operation).append(" partial failure (")
                .append(failures.size()).append('/').append(children.size()).append("): ");
        for (int i = 0; i < failures.size(); i++) {
            if (i > 0) {
                msg.append("; ");
            }
            msg.append(failures.get(i).describe());
        }
        msg.append("; progress=").append(progressSnapshot);
        MongoSyncException ex = new MongoSyncException(
                MongoSyncErrorCode.MULTI_OPERATION_PARTIAL_FAILURE,
                msg.toString(),
                failures.get(0).error);
        for (int i = 1; i < failures.size(); i++) {
            ex.addSuppressed(failures.get(i).error);
        }
        throw ex;
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
        if (left == MigrationState.PAUSED || right == MigrationState.PAUSED) {
            return MigrationState.PAUSED;
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

    private String phaseOf(MigrationState state, boolean fullComplete) {
        switch (state) {
            case IDLE:
                return "NOT_STARTED";
            case RUNNING:
                if (config.getSyncMode().includesFull() && !fullComplete) {
                    return "INITIAL_COPY";
                }
                if (config.getSyncMode().includesIncremental()) {
                    return "CHANGE_EVENT_APPLY";
                }
                return "RUNNING";
            case PAUSED:
                if (config.getSyncMode().includesFull() && !fullComplete) {
                    return "PAUSED_INITIAL_COPY";
                }
                if (config.getSyncMode().includesIncremental()) {
                    return "PAUSED_CHANGE_EVENT_APPLY";
                }
                return "PAUSED";
            case CAN_COMMIT:
                return "READY_TO_COMMIT";
            case COMMITTING:
                return "COMMITTING";
            case COMMITTED:
                return "COMMITTED";
            case STOPPED:
                return "STOPPED";
            case ERROR:
                return "ERROR";
            default:
                return "UNKNOWN";
        }
    }
}
