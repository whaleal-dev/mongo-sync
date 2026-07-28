package com.whaleal.third.mongo.source.sdk;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.whaleal.third.mongo.source.config.MongoSourceConfig;
import com.whaleal.third.mongo.source.converter.BsonToTransferEventConverter;
import com.whaleal.third.mongo.source.exception.SourceConnectException;
import com.whaleal.third.mongo.source.exception.SourceHistoryLostException;
import com.whaleal.third.mongo.source.exception.SourceOffsetException;
import com.whaleal.third.mongo.source.full.FullSyncRangeSplitter;
import com.whaleal.third.mongo.transfer.model.DdlEvent;
import com.whaleal.third.mongo.transfer.model.DdlType;
import com.whaleal.third.mongo.transfer.model.TransferEvent;
import com.whaleal.third.mongo.transfer.spi.TransferEventListener;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.conversions.Bson;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractSourceListener implements SourceListener {

    /** NamespaceNotFound */
    private static final int ERR_NAMESPACE_NOT_FOUND = 26;
    /** QueryPlanKilled（含集合被 drop 后游标失效） */
    private static final int ERR_QUERY_PLAN_KILLED = 175;

    protected final MongoSourceConfig config;
    protected final AtomicBoolean running = new AtomicBoolean(false);
    protected final AtomicBoolean stopped = new AtomicBoolean(false);
    /**
     * 增量侧识别到本表 ns 失效（drop / rename / dropDatabase）时置位：全量扫描视为完成并尽快退出。
     */
    private final AtomicBoolean fullSyncCompleteDueToNsChange = new AtomicBoolean(false);
    protected MongoClient mongoClient;
    private final boolean ownsMongoClient;
    private ExecutorService coordinatorExecutor;
    private ExecutorService incrementalExecutor;
    private ScheduledExecutorService offsetLogScheduler;
    /** 最近一次推进的位点摘要，供周期心跳 / 异常日志使用。 */
    private final AtomicReference<String> lastOffsetSnapshot = new AtomicReference<String>(null);

    protected AbstractSourceListener(MongoSourceConfig config) {
        this.config = config;
        if (config.getMongoClient() != null) {
            this.mongoClient = config.getMongoClient();
            this.ownsMongoClient = config.isCloseMongoClientOnStop();
        } else {
            this.mongoClient = null;
            this.ownsMongoClient = true;
        }
    }

    @Override
    public void start() {
        if (stopped.get()) {
            throw new IllegalStateException("CDC listener already stopped");
        }
        if (running.compareAndSet(false, true)) {
            startOffsetLogHeartbeat();
            coordinatorExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, listenerThreadName());
                t.setPriority(config.getListenerThreadPriority());
                return t;
            });

            coordinatorExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        runCapture();
                    } catch (SourceHistoryLostException | SourceConnectException | SourceOffsetException e) {
                        logOffsetSnapshot("fatal");
                        running.set(false);
                        throw e;
                    } catch (Exception e) {
                        logOffsetSnapshot("fatal");
                        running.set(false);
                        throw new SourceConnectException("Failed to start CDC listener", e);
                    }
                }
            });
        }
    }

    /**
     * 全量 + 增量模式：先定位点，再<strong>并行</strong>跑增量与全量；仅全量/仅增量则串行。
     */
    private void runCapture() throws Exception {
        boolean needFull = config.getSyncMode().includesFull();
        boolean needInc = config.getSyncMode().includesIncremental();

        if (needFull && needInc) {
            beforeInitialSync();
            incrementalExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, listenerThreadName() + "-incremental");
                t.setPriority(config.getListenerThreadPriority());
                return t;
            });
            Future<?> incrementalFuture = incrementalExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        startIncremental();
                    } catch (RuntimeException e) {
                        running.set(false);
                        throw e;
                    }
                }
            });
            try {
                performInitialSync();
                onInitialSyncCompleted();
                runAfterFullSyncBarrier();
                incrementalFuture.get();
            } catch (Exception e) {
                running.set(false);
                incrementalFuture.cancel(true);
                throw e;
            }
            return;
        }

        if (needFull) {
            beforeInitialSync();
            performInitialSync();
            onInitialSyncCompleted();
            runAfterFullSyncBarrier();
            running.set(false);
            return;
        }

        if (needInc) {
            startIncremental();
        } else {
            running.set(false);
        }
    }

    @Override
    public void stop() {
        if (stopped.compareAndSet(false, true)) {
            running.set(false);
            logOffsetSnapshot("stop");

            shutdownExecutor(offsetLogScheduler);
            shutdownExecutor(incrementalExecutor);
            shutdownExecutor(coordinatorExecutor);
            offsetLogScheduler = null;
            incrementalExecutor = null;
            coordinatorExecutor = null;

            if (ownsMongoClient && mongoClient != null) {
                try {
                    mongoClient.close();
                } catch (Exception ignored) {
                }
                mongoClient = null;
            }
        }
    }

    private static void shutdownExecutor(ExecutorService executor) {
        if (executor == null) {
            return;
        }
        executor.shutdownNow();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void startOffsetLogHeartbeat() {
        int intervalSec = config.getOffsetLogIntervalSeconds();
        if (intervalSec <= 0) {
            return;
        }
        offsetLogScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, listenerThreadName() + "-offset-log");
            t.setDaemon(true);
            return t;
        });
        offsetLogScheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (!running.get() || stopped.get()) {
                    return;
                }
                logOffsetSnapshot("heartbeat");
            }
        }, intervalSec, intervalSec, TimeUnit.SECONDS);
    }

    /**
     * 记录最近位点（内存快照）；周期性心跳与异常路径会打印到 stderr。
     *
     * @param kind        如 oplog / changeStream / full
     * @param syncTs      同步时间（oplog ts / clusterTime），可为 null
     * @param detail      额外信息（如 resume token 摘要）
     */
    protected void reportOffsetProgress(String kind, BsonTimestamp syncTs, String detail) {
        StringBuilder sb = new StringBuilder();
        sb.append("kind=").append(kind == null ? "?" : kind);
        sb.append(" ns=").append(config.getDatabase()).append('.').append(config.getCollection());
        if (syncTs != null) {
            sb.append(" syncTime=").append(formatBsonTimestamp(syncTs));
            sb.append(" ts={t=").append(syncTs.getTime()).append(",i=").append(syncTs.getInc()).append('}');
        } else {
            sb.append(" syncTime=unknown");
        }
        if (detail != null && !detail.isEmpty()) {
            sb.append(' ').append(detail);
        }
        lastOffsetSnapshot.set(sb.toString());
    }

    /** 立即打印当前位点快照（异常 / 停机 / 心跳）。 */
    protected void logOffsetSnapshot(String reason) {
        String snap = lastOffsetSnapshot.get();
        if (snap == null) {
            System.err.println("[mongo-source] offset " + reason + " ns="
                    + config.getDatabase() + '.' + config.getCollection()
                    + " syncTime=none (not advanced yet)");
            return;
        }
        System.err.println("[mongo-source] offset " + reason + " " + snap);
    }

    protected static String formatBsonTimestamp(BsonTimestamp ts) {
        if (ts == null) {
            return "null";
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(new Date(ts.getTime() * 1000L)) + "Z";
    }

    protected abstract String listenerThreadName();

    protected abstract void startIncremental();

    /**
     * 全量快照开始前钩子：记录增量起点（须在启动并行增量之前调用）。
     */
    protected void beforeInitialSync() {
    }

    protected abstract void onInitialSyncCompleted();

    /**
     * 全量扫描结束后的下游屏障（此时增量可能已在并行写入）。
     */
    protected void runAfterFullSyncBarrier() {
        Runnable barrier = config.getAfterFullSyncBarrier();
        if (barrier != null) {
            barrier.run();
        }
    }

    /**
     * 增量识别到本集合 ns 失效时调用：全量读取视为完成，扫描循环尽快退出，
     * 随后仍会走 {@link #onInitialSyncCompleted()} / {@link #runAfterFullSyncBarrier()}。
     * <p>
     * 覆盖：{@link DdlType#DROP_COLLECTION}、{@link DdlType#DROP_DATABASE}、
     * {@link DdlType#RENAME_COLLECTION}（from/to 触及监视集合）。
     */
    protected void maybeCompleteFullSyncOnNsChange(DdlEvent event) {
        if (event == null || event.getType() == null) {
            return;
        }
        switch (event.getType()) {
            case DROP_COLLECTION:
                if (matchesWatchedCollection(event)) {
                    fullSyncCompleteDueToNsChange.set(true);
                }
                break;
            case DROP_DATABASE:
                if (matchesWatchedDatabase(event)) {
                    fullSyncCompleteDueToNsChange.set(true);
                }
                break;
            case RENAME_COLLECTION:
                if (renameTouchesWatched(event)) {
                    fullSyncCompleteDueToNsChange.set(true);
                }
                break;
            default:
                break;
        }
    }

    /** @deprecated 使用 {@link #maybeCompleteFullSyncOnNsChange(DdlEvent)} */
    protected void maybeCompleteFullSyncOnDrop(DdlEvent event) {
        maybeCompleteFullSyncOnNsChange(event);
    }

    protected boolean shouldStopInitialSync() {
        return stopped.get() || fullSyncCompleteDueToNsChange.get();
    }

    protected boolean isFullSyncCompleteDueToDrop() {
        return fullSyncCompleteDueToNsChange.get();
    }

    protected boolean isFullSyncCompleteDueToNsChange() {
        return fullSyncCompleteDueToNsChange.get();
    }

    private boolean matchesWatchedCollection(DdlEvent event) {
        String coll = event.getCollection();
        if (coll == null || !config.getCollection().equals(coll)) {
            return false;
        }
        String db = event.getDatabase();
        return db == null || config.getDatabase().equals(db);
    }

    private boolean matchesWatchedDatabase(DdlEvent event) {
        String db = event.getDatabase();
        return db == null || config.getDatabase().equals(db);
    }

    private boolean renameTouchesWatched(DdlEvent event) {
        String watchedNs = config.getDatabase() + "." + config.getCollection();
        String watchedColl = config.getCollection();
        BsonDocument cmd = event.getCommand();
        if (cmd != null) {
            if (cmd.containsKey("renameCollection") && cmd.get("renameCollection").isString()) {
                String from = cmd.getString("renameCollection").getValue();
                if (watchedNs.equals(from) || watchedColl.equals(from)) {
                    return true;
                }
            }
            if (cmd.containsKey("to") && cmd.get("to").isString()) {
                String to = cmd.getString("to").getValue();
                if (watchedNs.equals(to)
                        || watchedColl.equals(to)
                        || to.endsWith("." + watchedColl)) {
                    return true;
                }
            }
        }
        return matchesWatchedCollection(event);
    }

    protected void performInitialSync() {
        if (fullSyncCompleteDueToNsChange.get()) {
            return;
        }
        ensureConnection();
        MongoDatabase database = mongoClient.getDatabase(config.getDatabase());
        MongoCollection<BsonDocument> collection = database.getCollection(config.getCollection(), BsonDocument.class);

        BsonToTransferEventConverter converter = new BsonToTransferEventConverter(config.getDatabase(), config.getCollection());
        TransferEventListener listener = config.getListener();
        int parallelism = Math.max(1, config.getFullSyncParallelism());
        int cursorBatch = Math.max(1, config.getFullSyncBatchSize());

        try {
            if (parallelism <= 1) {
                scanRange(collection, FullSyncRangeSplitter.IdRange.all(), converter, listener, cursorBatch);
                return;
            }

            // 对齐 d2t：先按 _id 切多段 Range，再以 readThreadNum 并行拉取
            List<FullSyncRangeSplitter.IdRange> ranges = FullSyncRangeSplitter.split(
                    database, collection, config.getFullSyncTaskMbSize());
            if (ranges.isEmpty()) {
                return;
            }
            if (ranges.size() == 1) {
                scanRange(collection, ranges.get(0), converter, listener, cursorBatch);
                return;
            }

            System.err.println("[mongo-source] full-sync parallel readers=" + parallelism
                    + " ranges=" + ranges.size()
                    + " ns=" + config.getDatabase() + "." + config.getCollection());
            ExecutorService pool = Executors.newFixedThreadPool(parallelism);
            List<Future<?>> futures = new ArrayList<Future<?>>(ranges.size());
            final AtomicReference<RuntimeException> failure = new AtomicReference<RuntimeException>();
            try {
                for (final FullSyncRangeSplitter.IdRange range : ranges) {
                    futures.add(pool.submit(new Callable<Void>() {
                        @Override
                        public Void call() {
                            if (shouldStopInitialSync() || failure.get() != null) {
                                return null;
                            }
                            try {
                                scanRange(collection, range, converter, listener, cursorBatch);
                            } catch (RuntimeException e) {
                                failure.compareAndSet(null, e);
                                throw e;
                            }
                            return null;
                        }
                    }));
                }
                for (Future<?> f : futures) {
                    try {
                        f.get();
                    } catch (Exception e) {
                        Throwable cause = e.getCause() != null ? e.getCause() : e;
                        if (cause instanceof RuntimeException) {
                            failure.compareAndSet(null, (RuntimeException) cause);
                        } else {
                            failure.compareAndSet(null, new SourceConnectException(
                                    "full-sync parallel range failed", cause));
                        }
                    }
                }
                RuntimeException err = failure.get();
                if (err != null) {
                    throw err;
                }
            } finally {
                pool.shutdownNow();
            }
        } catch (MongoException e) {
            // 扫描中途表被删/改名：视为全量完成，不抛失败
            if (fullSyncCompleteDueToNsChange.get() || isNamespaceGone(e)) {
                fullSyncCompleteDueToNsChange.set(true);
                System.err.println("[mongo-source] initial sync treated complete after ns change: "
                        + config.getDatabase() + "." + config.getCollection()
                        + " code=" + e.getCode());
                return;
            }
            throw e;
        }
    }

    private void scanRange(MongoCollection<BsonDocument> collection,
                           FullSyncRangeSplitter.IdRange range,
                           BsonToTransferEventConverter converter,
                           TransferEventListener listener,
                           int cursorBatch) {
        Bson filter = range.toFilter();
        try (MongoCursor<BsonDocument> cursor = collection.find(filter)
                .sort(new org.bson.Document("_id", 1))
                .batchSize(cursorBatch)
                .iterator()) {
            while (cursor.hasNext() && !shouldStopInitialSync()) {
                BsonDocument document = cursor.next();
                if (shouldStopInitialSync()) {
                    break;
                }
                TransferEvent event = converter.createSnapshotEvent(document);
                listener.onEvent(event);
            }
        } catch (MongoException e) {
            if (fullSyncCompleteDueToNsChange.get() || isNamespaceGone(e)) {
                fullSyncCompleteDueToNsChange.set(true);
                System.err.println("[mongo-source] range scan treated complete after ns change: "
                        + range + " code=" + e.getCode());
                return;
            }
            throw e;
        }
    }

    private static boolean isNamespaceGone(MongoException e) {
        int code = e.getCode();
        if (code == ERR_NAMESPACE_NOT_FOUND || code == ERR_QUERY_PLAN_KILLED) {
            return true;
        }
        String msg = e.getMessage();
        if (msg == null) {
            return false;
        }
        String lower = msg.toLowerCase();
        return lower.contains("ns not found")
                || lower.contains("namespace not found")
                || lower.contains("namespace does not exist")
                || lower.contains("collection dropped")
                || lower.contains("collection not found")
                || lower.contains("collection renamed")
                || lower.contains("was renamed");
    }

    protected void ensureConnection() {
        if (mongoClient == null) {
            try {
                mongoClient = MongoClients.create(config.getUri());
            } catch (Exception e) {
                throw new SourceConnectException("Failed to connect to MongoDB", e);
            }
        }
    }

    protected int handleRetry(Exception e, int currentRetryCount) {
        if (stopped.get()) {
            return currentRetryCount;
        }
        logOffsetSnapshot("retry err=" + (e == null ? "null" : e.getClass().getSimpleName()
                + (e.getMessage() == null ? "" : (":" + e.getMessage()))));

        if (e instanceof SourceHistoryLostException) {
            throw (SourceHistoryLostException) e;
        }

        if (e instanceof MongoException) {
            MongoException mongoException = (MongoException) e;
            if (isHistoryLostException(mongoException)) {
                throw new SourceHistoryLostException("Change stream history lost, resume token expired", e);
            }
        }

        if (currentRetryCount >= config.getRetryMaxTimes()) {
            throw new SourceConnectException("Max retry attempts reached", e);
        }

        long delay = calculateRetryDelay(currentRetryCount);
        try {
            Thread.sleep(delay);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }

        return currentRetryCount + 1;
    }

    protected long calculateRetryDelay(int retryCount) {
        long baseDelay = config.getRetryIntervalMs();
        return baseDelay * (long) Math.pow(2, retryCount);
    }

    protected boolean isHistoryLostException(MongoException e) {
        int errorCode = e.getCode();
        return errorCode == 234 || errorCode == 136 || errorCode == 203;
    }
}
