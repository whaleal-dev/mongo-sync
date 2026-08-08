package com.whaleal.third.mongo.sync.sdk;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.whaleal.third.mongo.sink.sdk.MongoSinkClient;
import com.whaleal.third.mongo.source.config.CaptureMode;
import com.whaleal.third.mongo.source.config.SyncMode;
import com.whaleal.third.mongo.source.oplog.MongoVersion;
import com.whaleal.third.mongo.source.sdk.MongoSourceClient;
import com.whaleal.third.mongo.source.spi.OplogOffsetStorage;
import com.whaleal.third.mongo.source.topology.SourceTopology;
import com.whaleal.third.mongo.source.topology.SourceTopologyDetector;
import com.whaleal.third.mongo.source.topology.SourceTopologyInfo;
import com.whaleal.third.mongo.sync.cache.SyncCaches;
import com.whaleal.third.mongo.sync.config.MongoSyncConfig;
import com.whaleal.third.mongo.sync.error.MongoSyncErrorCode;
import com.whaleal.third.mongo.sync.error.MongoSyncException;
import com.whaleal.third.mongo.sync.meta.CollectionStructureBootstrap;
import com.whaleal.third.mongo.sync.offset.FileOffsetStoreFactory;
import com.whaleal.third.mongo.sync.offset.MemoryOplogOffsetStorage;
import com.whaleal.third.mongo.sync.offset.MemoryResumeTokenStorage;
import com.whaleal.third.mongo.sync.pipeline.BucketWritePipeline;
import com.whaleal.third.mongo.sync.pipeline.IdBucketRouter;
import com.whaleal.third.mongo.transfer.model.DdlEvent;
import com.whaleal.third.mongo.transfer.model.TransferEvent;
import com.whaleal.third.mongo.transfer.spi.DdlEventListener;
import com.whaleal.third.mongo.transfer.spi.TransferEventListener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 文档库同步客户端（参考 d2t 实时同步链路）：
 * <pre>
 * Source(Oplog/ChangeStream)
 *   → TransferEvent / DdlEvent
 *   → 分桶（_id hash）+ Caffeine ns/DDL 锁
 *   → Sink 落地
 * </pre>
 * <p>
 * 源端架构自动匹配（{@code captureMode=AUTO}，默认）：
 * 副本集优先 ChangeStream，低于 3.6 回落 OPLOG；分片集群只走 ChangeStream@mongos；
 * standalone 仅支持全量。
 */
public final class MongoSyncClient implements AutoCloseable {

    private final MongoSyncConfig config;
    private final MongoClient sourceClient;
    private final MongoClient targetClient;
    private final boolean ownsSourceClient;
    private final boolean ownsTargetClient;
    private final SyncCaches caches;
    private final BucketWritePipeline pipeline;
    private final MongoSinkClient sink;
    /** 解析后的捕获模式（AUTO 已展开）。 */
    private final CaptureMode resolvedCaptureMode;
    private final SourceTopology sourceTopology;
    private final MongoSourceClient source;
    private final AtomicReference<MigrationState> migrationState =
            new AtomicReference<MigrationState>(MigrationState.IDLE);
    private final AtomicLong snapshotEvents = new AtomicLong(0);
    private final AtomicLong incrementalEvents = new AtomicLong(0);
    private final AtomicLong ddlEvents = new AtomicLong(0);
    private final AtomicLong startedAtMs = new AtomicLong(0);
    private final AtomicLong estimatedTotalDocuments = new AtomicLong(0);
    private final AtomicReference<Long> committedAtMs = new AtomicReference<Long>(null);
    private final AtomicReference<Long> lastEventTsMs = new AtomicReference<Long>(null);
    /** 最近一次收到增量事件的本机时间（用于 idle 源判定追平）。 */
    private final AtomicLong lastIncrementalReceivedAtMs = new AtomicLong(0L);
    private final AtomicBoolean fullSyncComplete = new AtomicBoolean(false);
    private final AtomicReference<String> stateDetail = new AtomicReference<String>("idle");
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final AtomicBoolean paused = new AtomicBoolean(false);

    private MongoSyncClient(MongoSyncConfig config) {
        this.config = config;
        if (config.getSourceMongoClient() != null) {
            this.sourceClient = config.getSourceMongoClient();
            this.ownsSourceClient = config.isCloseSourceClientOnStop();
        } else {
            this.sourceClient = MongoClients.create(config.getSourceUri());
            this.ownsSourceClient = true;
        }
        if (config.getTargetMongoClient() != null) {
            this.targetClient = config.getTargetMongoClient();
            this.ownsTargetClient = config.isCloseTargetClientOnClose();
        } else {
            this.targetClient = MongoClients.create(config.getTargetUri());
            this.ownsTargetClient = true;
        }

        this.caches = new SyncCaches(config.getNsLockExpireMinutes());
        estimateTotalDocuments();
        BucketWritePipeline.probeUniqueIndexes(
                sourceClient,
                config.getSourceDatabase(),
                config.getSourceCollection(),
                config.sourceNs(),
                caches);

        boolean orderedWrite = caches.hasUniqueIndex(config.sourceNs());

        this.sink = MongoSinkClient.builder()
                .mongoClient(targetClient)
                .closeMongoClientOnClose(false)
                .database(config.getTargetDatabase())
                .collection(config.getTargetCollection())
                .writeMode(config.getWriteMode())
                .onConflict(config.getOnConflict())
                .batchSize(config.getTargetBatchSize())
                .writerThreads(config.getTargetWriterThreads())
                .ordered(orderedWrite)
                .build();

        IdBucketRouter router = new IdBucketRouter(
                config.getBucketNum(),
                config.isForceSingleBucketOnUniqueIndex(),
                caches,
                config.sourceNs());

        this.pipeline = new BucketWritePipeline(
                sink,
                router,
                caches,
                sourceClient,
                config.getSourceDatabase(),
                config.getSourceCollection(),
                config.sourceNs(),
                config.getBucketNum(),
                config.getBucketQueueCapacity(),
                config.getDdlWaitSeconds(),
                config.getWriteErrorHandler());

        TransferEventListener eventListener = new TransferEventListener() {
            @Override
            public void onEvent(TransferEvent event) {
                if (stopped.get() || event == null) {
                    return;
                }
                if ("r".equals(event.getOp())) {
                    snapshotEvents.incrementAndGet();
                } else {
                    incrementalEvents.incrementAndGet();
                    lastIncrementalReceivedAtMs.set(System.currentTimeMillis());
                }
                if (event.getTsMs() != null) {
                    lastEventTsMs.set(event.getTsMs());
                }
                pipeline.offer(event);
                refreshCommitState();
            }
        };

        DdlEventListener ddlListener = new DdlEventListener() {
            @Override
            public void onDdl(DdlEvent event) {
                ddlEvents.incrementAndGet();
                pipeline.applyDdl(event);
                refreshCommitState();
            }
        };

        Runnable afterFull = new Runnable() {
            @Override
            public void run() {
                pipeline.tryDrainAndFlush(config.getDdlWaitSeconds());
                fullSyncComplete.set(true);
                stateDetail.set("full-sync complete");
                refreshCommitState();
            }
        };

        // 按源端架构自动匹配读任务：standalone / replicaSet / sharding
        SourceTopologyInfo topologyInfo = SourceTopologyDetector.detect(
                sourceClient,
                config.getCaptureMode(),
                config.getSourceUri(),
                config.getSyncMode());
        this.sourceTopology = topologyInfo.getTopology();
        this.resolvedCaptureMode = topologyInfo.getResolvedCaptureMode();
        MongoVersion resolvedVersion = config.getMongoVersion() != null
                ? config.getMongoVersion()
                : topologyInfo.getVersion();

        // 单源：RS(ChangeStream 或 OPLOG) / standalone(FULL) / sharding+ChangeStream@mongos
        System.err.println("[mongo-sync] read-plan " + sourceTopology
                + ": single-source capture=" + resolvedCaptureMode
                + " syncMode=" + config.getSyncMode()
                + " ns=" + config.sourceNs());
        this.source = buildSource(
                sourceClient,
                config.getSyncMode(),
                resolvedCaptureMode,
                resolvedVersion,
                eventListener,
                ddlListener,
                afterFull,
                resolveSingleOplogStorage(config, resolvedCaptureMode));
    }

    private MongoSourceClient buildSource(MongoClient client,
                                          SyncMode syncMode,
                                          CaptureMode captureMode,
                                          MongoVersion mongoVersion,
                                          TransferEventListener eventListener,
                                          DdlEventListener ddlListener,
                                          Runnable afterFull,
                                          OplogOffsetStorage oplogOffsetStorage) {
        MongoSourceClient.Builder sourceBuilder = MongoSourceClient.builder()
                .mongoClient(client)
                .closeMongoClientOnStop(false)
                .database(config.getSourceDatabase())
                .collection(config.getSourceCollection())
                .captureMode(captureMode)
                .syncMode(syncMode)
                .fullDocument(config.getFullDocument())
                .enablePreImage(config.isEnablePreImage())
                .includeFromMigrate(config.isIncludeFromMigrate())
                .listener(eventListener)
                .ddlListener(ddlListener)
                .offsetLogIntervalSeconds(config.getOffsetLogIntervalSeconds())
                .fullSyncParallelism(config.getFullSyncParallelism())
                .fullSyncBatchSize(config.getFullSyncBatchSize())
                .fullSyncTaskMbSize(config.getFullSyncTaskMbSize());

        if (afterFull != null) {
            sourceBuilder.afterFullSyncBarrier(afterFull);
        }
        if (mongoVersion != null) {
            sourceBuilder.mongoVersion(mongoVersion);
        }
        if (config.getOplogStartTimestamp() != null) {
            sourceBuilder.oplogStartTimestamp(config.getOplogStartTimestamp());
        }
        if (config.getOplogEndTimestamp() != null) {
            sourceBuilder.oplogEndTimestamp(config.getOplogEndTimestamp());
        }

        if (captureMode == CaptureMode.OPLOG) {
            sourceBuilder.oplogOffsetStorage(
                    oplogOffsetStorage != null ? oplogOffsetStorage : new MemoryOplogOffsetStorage());
        } else {
            if (config.getResumeTokenStorage() != null) {
                sourceBuilder.resumeTokenStorage(config.getResumeTokenStorage());
            } else if (config.getOffsetStoreDir() != null && !config.getOffsetStoreDir().trim().isEmpty()) {
                sourceBuilder.resumeTokenStorage(new FileOffsetStoreFactory(config.getOffsetStoreDir().trim())
                        .resumeTokenStorage(config.getSourceDatabase(), config.getSourceCollection()));
            } else {
                sourceBuilder.resumeTokenStorage(new MemoryResumeTokenStorage());
            }
        }

        return sourceBuilder.build();
    }

    private OplogOffsetStorage resolveSingleOplogStorage(MongoSyncConfig config, CaptureMode captureMode) {
        if (captureMode != CaptureMode.OPLOG) {
            return null;
        }
        if (config.getOplogOffsetStorage() != null) {
            return config.getOplogOffsetStorage();
        }
        if (config.getOffsetStoreDir() != null && !config.getOffsetStoreDir().trim().isEmpty()) {
            return new FileOffsetStoreFactory(config.getOffsetStoreDir().trim())
                    .oplogOffsetStorage(config.getSourceDatabase(), config.getSourceCollection());
        }
        return new MemoryOplogOffsetStorage();
    }

    public static MongoSyncConfig.Builder builder() {
        return MongoSyncConfig.builder();
    }

    public static MongoSyncClient create(MongoSyncConfig.Builder configBuilder) {
        return create(configBuilder.build());
    }

    public static MongoSyncClient create(MongoSyncConfig config) {
        return new MongoSyncClient(config);
    }

    public void start() {
        if (stopped.get()) {
            throw new MongoSyncException(MongoSyncErrorCode.CLIENT_STATE_INVALID,
                    "sync client already stopped");
        }
        if (paused.compareAndSet(true, false)) {
            if (config.getSyncMode().includesFull() && !fullSyncComplete.get()) {
                paused.set(true);
                throw new MongoSyncException(MongoSyncErrorCode.RESUME_NOT_ALLOWED,
                        "resume during initial full sync is not supported; restart migration or wait until full sync completes");
            }
            migrationState.set(MigrationState.RUNNING);
            stateDetail.set("resuming");
            if (source != null) {
                source.start();
            }
            stateDetail.set("running");
            refreshCommitState();
            return;
        }
        if (started.compareAndSet(false, true)) {
            startedAtMs.compareAndSet(0, System.currentTimeMillis());
            migrationState.set(MigrationState.RUNNING);
            stateDetail.set("starting");
            if (config.isBootstrapCollection() || config.isBootstrapIndexes()) {
                CollectionStructureBootstrap.ensureTarget(
                        sourceClient,
                        targetClient,
                        config.getSourceDatabase(),
                        config.getSourceCollection(),
                        config.getTargetDatabase(),
                        config.getTargetCollection(),
                        config.isBootstrapCollection(),
                        config.isBootstrapIndexes(),
                        config.isSkipTtlIndexes());
            }
            if (source != null) {
                source.start();
            }
            if (!config.getSyncMode().includesFull()) {
                fullSyncComplete.set(true);
            }
            stateDetail.set("running");
            refreshCommitState();
        }
    }

    public void resume() {
        start();
    }

    public synchronized MigrationProgress pause() {
        if (stopped.get()) {
            throw new MongoSyncException(MongoSyncErrorCode.CLIENT_STATE_INVALID,
                    "sync client already stopped");
        }
        MigrationState current = migrationState.get();
        if (current == MigrationState.COMMITTED || current == MigrationState.COMMITTING) {
            throw new MongoSyncException(MongoSyncErrorCode.PAUSE_NOT_ALLOWED,
                    "cannot pause in state=" + current);
        }
        if (started.get() && config.getSyncMode().includesFull() && !fullSyncComplete.get()) {
            throw new MongoSyncException(MongoSyncErrorCode.PAUSE_NOT_ALLOWED,
                    "pause during initial full sync is blocked to avoid snapshot replay/duplication; wait until full sync completes");
        }
        if (!started.get()) {
            throw new MongoSyncException(MongoSyncErrorCode.PAUSE_NOT_ALLOWED,
                    "cannot pause before start; start the migration first");
        }
        if (!paused.compareAndSet(false, true)) {
            return progress();
        }
        migrationState.set(MigrationState.PAUSED);
        stateDetail.set("pausing");
        if (source != null) {
            source.pause();
        }
        pipeline.tryDrainAndFlush(Math.max(config.getDdlWaitSeconds(), 30));
        stateDetail.set("paused");
        return progress();
    }

    public void stop() {
        close();
    }

    public long inflightEvents() {
        return pipeline.inflight();
    }

    public boolean canCommit() {
        MigrationState state = migrationState.get();
        return state == MigrationState.CAN_COMMIT || state == MigrationState.COMMITTED;
    }

    public MigrationProgress progress() {
        MigrationState state = migrationState.get();
        boolean canCommit = state == MigrationState.CAN_COMMIT || state == MigrationState.COMMITTED;
        long now = System.currentTimeMillis();
        Long lagMs = computeLagMs(now);
        return new MigrationProgress(
                config.sourceNs(),
                phaseOf(state),
                sourceTopology == null ? "UNKNOWN" : sourceTopology.name(),
                resolvedCaptureMode == null ? "UNKNOWN" : resolvedCaptureMode.name(),
                config.getSyncMode() == null ? "UNKNOWN" : config.getSyncMode().name(),
                state,
                canCommit,
                fullSyncComplete.get(),
                estimatedTotalDocuments.get(),
                snapshotEvents.get(),
                incrementalEvents.get(),
                ddlEvents.get(),
                pipeline.inflight(),
                lastEventTsMs.get(),
                startedAtMs.get(),
                committedAtMs.get(),
                startedAtMs.get() > 0 ? (now - startedAtMs.get()) : 0,
                lagMs,
                1,
                stateDetail.get(),
                commitReadinessOf(state));
    }

    /**
     * 最小 cutover：要求已到可提交状态，然后排空写入并停止同步。
     */
    public synchronized MigrationProgress commit() {
        refreshCommitState();
        MigrationState current = migrationState.get();
        if (current == MigrationState.COMMITTED) {
            return progress();
        }
        if (current != MigrationState.CAN_COMMIT) {
            throw new MongoSyncException(MongoSyncErrorCode.COMMIT_NOT_ALLOWED,
                    "cannot commit in state=" + current
                            + ", readiness=" + commitReadinessOf(current)
                            + ", progress=" + progress());
        }
        migrationState.set(MigrationState.COMMITTING);
        stateDetail.set("committing");
        try {
            if (source != null) {
                source.stop();
            }
            pipeline.drainAndFlush(Math.max(config.getDdlWaitSeconds(), 30));
            committedAtMs.set(System.currentTimeMillis());
            migrationState.set(MigrationState.COMMITTED);
            stateDetail.set("committed");
            return progress();
        } catch (RuntimeException e) {
            migrationState.set(MigrationState.ERROR);
            stateDetail.set("commit failed: " + e.getMessage());
            throw e;
        }
    }

    /** 源端架构（standalone / replicaSet / sharding）。 */
    public SourceTopology getSourceTopology() {
        return sourceTopology;
    }

    /** AUTO 展开后的捕获模式。 */
    public CaptureMode getResolvedCaptureMode() {
        return resolvedCaptureMode;
    }

    public MongoSyncConfig getConfig() {
        return config;
    }

    private void estimateTotalDocuments() {
        try {
            MongoCollection<org.bson.Document> coll = sourceClient
                    .getDatabase(config.getSourceDatabase())
                    .getCollection(config.getSourceCollection());
            estimatedTotalDocuments.set(Math.max(0L, coll.estimatedDocumentCount()));
        } catch (Exception ignored) {
            estimatedTotalDocuments.set(0L);
        }
    }

    private void refreshCommitState() {
        MigrationState current = migrationState.get();
        if (current == MigrationState.COMMITTED
                || current == MigrationState.COMMITTING
                || current == MigrationState.ERROR
                || current == MigrationState.STOPPED
                || current == MigrationState.PAUSED) {
            return;
        }
        boolean ready = started.get()
                && fullSyncComplete.get()
                && pipeline.inflight() == 0
                && lagAcceptable(System.currentTimeMillis());
        MigrationState next = ready ? MigrationState.CAN_COMMIT : MigrationState.RUNNING;
        // 仅状态跃迁时更新 detail，避免热路径每条事件做字符串拼接
        if (current != next) {
            migrationState.set(next);
            stateDetail.set(ready ? "ready to commit" : commitReadinessOf(MigrationState.RUNNING));
        }
    }

    private String phaseOf(MigrationState state) {
        switch (state) {
            case IDLE:
                return "NOT_STARTED";
            case RUNNING:
                if (config.getSyncMode().includesFull() && !fullSyncComplete.get()) {
                    return "INITIAL_COPY";
                }
                if (config.getSyncMode().includesIncremental()) {
                    return "CHANGE_EVENT_APPLY";
                }
                return "RUNNING";
            case PAUSED:
                if (config.getSyncMode().includesFull() && !fullSyncComplete.get()) {
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

    private Long computeLagMs(long now) {
        Long last = lastEventTsMs.get();
        if (last == null || last.longValue() <= 0L) {
            return null;
        }
        long lag = now - last.longValue();
        return lag < 0L ? 0L : lag;
    }

    /**
     * 增量滞后是否可接受：clusterTime 滞后 ≤ {@link MongoSyncConfig#getCommitMaxLagMs()}，
     * 或增量流已空闲（无新事件 ≥ maxLag 且 pipeline 排空，适用于源端无写入）。
     */
    private boolean lagAcceptable(long now) {
        if (!config.getSyncMode().includesIncremental()) {
            return true;
        }
        long maxLag = config.getCommitMaxLagMs();
        if (lastIncrementalReceivedAtMs.get() <= 0L) {
            return false;
        }
        if (pipeline.inflight() == 0L && (now - lastIncrementalReceivedAtMs.get()) >= maxLag) {
            return true;
        }
        Long lag = computeLagMs(now);
        return lag != null && lag.longValue() <= maxLag;
    }

    private String commitReadinessOf(MigrationState state) {
        if (state == MigrationState.COMMITTED) {
            return "already committed";
        }
        if (state == MigrationState.COMMITTING) {
            return "commit in progress";
        }
        if (state == MigrationState.CAN_COMMIT) {
            return "ready";
        }
        if (!started.get()) {
            return "not started";
        }
        if (paused.get()) {
            return "paused";
        }
        if (!fullSyncComplete.get()) {
            return "waiting full sync completion";
        }
        long inflight = pipeline.inflight();
        if (inflight > 0L) {
            return "waiting inflight drain=" + inflight;
        }
        if (config.getSyncMode().includesIncremental()) {
            long maxLag = config.getCommitMaxLagMs();
            long lastReceived = lastIncrementalReceivedAtMs.get();
            if (lastReceived <= 0L) {
                return "waiting first incremental event";
            }
            long now = System.currentTimeMillis();
            if ((now - lastReceived) < maxLag) {
                Long lag = computeLagMs(now);
                if (lag == null || lag.longValue() > maxLag) {
                    return "waiting lagMs=" + (lag == null ? "unknown" : lag) + " maxLagMs=" + maxLag;
                }
            }
        }
        return "waiting prerequisites";
    }

    @Override
    public void close() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        if (migrationState.get() != MigrationState.COMMITTED) {
            migrationState.set(MigrationState.STOPPED);
            stateDetail.set("stopped");
        }
        if (source != null) {
            try {
                source.stop();
            } catch (Exception ignored) {
            }
        }
        try {
            pipeline.close();
        } catch (Exception ignored) {
        }
        try {
            sink.close();
        } catch (Exception ignored) {
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
}
