package com.whaleal.third.mongo.source.config;

import com.mongodb.client.MongoClient;
import com.whaleal.third.mongo.source.oplog.MongoVersion;
import com.whaleal.third.mongo.source.oplog.OplogFormatVersion;
import com.whaleal.third.mongo.transfer.spi.DdlEventListener;
import com.whaleal.third.mongo.transfer.spi.TransferEventListener;
import com.whaleal.third.mongo.source.spi.OplogOffsetStorage;
import com.whaleal.third.mongo.source.spi.ResumeTokenStorage;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import java.util.List;

public class MongoSourceConfig {

    private String uri;
    private MongoClient mongoClient;
    private boolean closeMongoClientOnStop;
    private String database;
    private String collection;
    private CaptureMode captureMode;
    private MongoVersion mongoVersion;
    private OplogFormatVersion oplogFormatVersion;
    private FullDocumentMode fullDocument;
    private boolean enablePreImage;
    private List<BsonDocument> pipeline;
    private int retryMaxTimes;
    private long retryIntervalMs;
    private SyncMode syncMode = SyncMode.INCREMENTAL;
    private ResumeTokenStorage resumeTokenStorage;
    private OplogOffsetStorage oplogOffsetStorage;
    private TransferEventListener listener;
    private DdlEventListener ddlListener;
    private int listenerThreadPriority;
    private BsonTimestamp oplogStartTimestamp;
    private BsonTimestamp oplogEndTimestamp;
    private int oplogBatchSize;
    private boolean includeFromMigrate;
    /** 全量扫描结束后的屏障（全量+增量并行时：增量可能已在跑；用于尽力 flush 积压）。 */
    private Runnable afterFullSyncBarrier;
    public static final int DEFAULT_RETRY_MAX_TIMES = 10;
    public static final long DEFAULT_RETRY_INTERVAL_MS = 1000;
    public static final int DEFAULT_THREAD_PRIORITY = Thread.NORM_PRIORITY;
    public static final int DEFAULT_OPLOG_BATCH_SIZE = 1024;
    public static final int DEFAULT_OFFSET_LOG_INTERVAL_SECONDS = 30;
    /** 全量并行读线程数；1 表示单游标（不拆分并行）。对齐 d2t sourceThreadNum。 */
    public static final int DEFAULT_FULL_SYNC_PARALLELISM = 1;
    public static final int DEFAULT_FULL_SYNC_BATCH_SIZE = 1000;
    /** 单段全量任务目标体积（MB），对齐 d2t SpliceNsData.mbSize。 */
    public static final int DEFAULT_FULL_SYNC_TASK_MB_SIZE = 32;

    /**
     * 周期性打印位点到 stderr 的间隔（秒）。默认 30；{@code <=0} 关闭。
     * 异常排查时可从日志看到上次同步时间。
     */
    private int offsetLogIntervalSeconds = DEFAULT_OFFSET_LOG_INTERVAL_SECONDS;
    /** 大表全量并行读线程数（>1 时按 _id 切段并行扫描）。 */
    private int fullSyncParallelism = DEFAULT_FULL_SYNC_PARALLELISM;
    /** 全量游标 batchSize。 */
    private int fullSyncBatchSize = DEFAULT_FULL_SYNC_BATCH_SIZE;
    /** 单段任务目标数据量（MB），用于估算 skip 切段大小。 */
    private int fullSyncTaskMbSize = DEFAULT_FULL_SYNC_TASK_MB_SIZE;

    private MongoSourceConfig() {
    }

    public String getUri() {
        return uri;
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public boolean isCloseMongoClientOnStop() {
        return closeMongoClientOnStop;
    }

    public String getDatabase() {
        return database;
    }

    public String getCollection() {
        return collection;
    }

    public CaptureMode getCaptureMode() {
        return captureMode;
    }

    public MongoVersion getMongoVersion() {
        return mongoVersion;
    }

    public OplogFormatVersion getOplogFormatVersion() {
        return oplogFormatVersion;
    }

    public FullDocumentMode getFullDocument() {
        return fullDocument;
    }

    public boolean isEnablePreImage() {
        return enablePreImage;
    }

    public List<BsonDocument> getPipeline() {
        return pipeline;
    }

    public int getRetryMaxTimes() {
        return retryMaxTimes;
    }

    public long getRetryIntervalMs() {
        return retryIntervalMs;
    }

    public SyncMode getSyncMode() {
        return syncMode == null ? SyncMode.INCREMENTAL : syncMode;
    }

    public boolean isCatchUpThenStop() {
        return getSyncMode().catchUpThenStop();
    }

    /**
     * ChangeStream 位点存储（ResumeToken）。
     */
    public ResumeTokenStorage getResumeTokenStorage() {
        return resumeTokenStorage;
    }

    /**
     * Oplog 位点存储（时间戳）。
     */
    public OplogOffsetStorage getOplogOffsetStorage() {
        return oplogOffsetStorage;
    }

    public TransferEventListener getListener() {
        return listener;
    }

    public DdlEventListener getDdlListener() {
        return ddlListener;
    }

    public int getListenerThreadPriority() {
        return listenerThreadPriority;
    }

    public BsonTimestamp getOplogStartTimestamp() {
        return oplogStartTimestamp;
    }

    /**
     * 增量结束位点（含）：用于 {@link SyncMode#FULL_AND_CATCH_UP} 或有界 {@link SyncMode#INCREMENTAL}。
     */
    public BsonTimestamp getOplogEndTimestamp() {
        return oplogEndTimestamp;
    }

    public int getOplogBatchSize() {
        return oplogBatchSize;
    }

    public boolean isIncludeFromMigrate() {
        return includeFromMigrate;
    }

    public Runnable getAfterFullSyncBarrier() {
        return afterFullSyncBarrier;
    }

    public int getOffsetLogIntervalSeconds() {
        return offsetLogIntervalSeconds;
    }

    /** 全量并行读线程数；1 为单游标。 */
    public int getFullSyncParallelism() {
        return fullSyncParallelism;
    }

    /** 全量游标 batchSize。 */
    public int getFullSyncBatchSize() {
        return fullSyncBatchSize;
    }

    /** 单段全量任务目标体积（MB）。 */
    public int getFullSyncTaskMbSize() {
        return fullSyncTaskMbSize;
    }

    public static Builder builder() {
        return new Builder();
    }

    public enum FullDocumentMode {
        DEFAULT,
        UPDATE_LOOKUP,
        WHEN_AVAILABLE,
        REQUIRED
    }

    public static class Builder {
        private String uri;
        private MongoClient mongoClient;
        private boolean closeMongoClientOnStop = true;
        private String database;
        private String collection;
        private CaptureMode captureMode = CaptureMode.AUTO;
        private MongoVersion mongoVersion;
        private FullDocumentMode fullDocument = FullDocumentMode.DEFAULT;
        private boolean enablePreImage = false;
        private List<BsonDocument> pipeline;
        private int retryMaxTimes = DEFAULT_RETRY_MAX_TIMES;
        private long retryIntervalMs = DEFAULT_RETRY_INTERVAL_MS;
        private SyncMode syncMode = SyncMode.INCREMENTAL;
        private ResumeTokenStorage resumeTokenStorage;
        private OplogOffsetStorage oplogOffsetStorage;
        private TransferEventListener listener;
        private DdlEventListener ddlListener;
        private int listenerThreadPriority = DEFAULT_THREAD_PRIORITY;
        private BsonTimestamp oplogStartTimestamp;
        private BsonTimestamp oplogEndTimestamp;
        private int oplogBatchSize = DEFAULT_OPLOG_BATCH_SIZE;
        private boolean includeFromMigrate = false;
        private Runnable afterFullSyncBarrier;
        private int offsetLogIntervalSeconds = DEFAULT_OFFSET_LOG_INTERVAL_SECONDS;
        private int fullSyncParallelism = DEFAULT_FULL_SYNC_PARALLELISM;
        private int fullSyncBatchSize = DEFAULT_FULL_SYNC_BATCH_SIZE;
        private int fullSyncTaskMbSize = DEFAULT_FULL_SYNC_TASK_MB_SIZE;

        public Builder uri(String uri) {
            this.uri = uri;
            return this;
        }

        /**
         * 复用已有 MongoClient（连接池常驻）。设置后不再按 uri 新建；
         * 默认 stop 时不关闭该外部 client。
         */
        public Builder mongoClient(MongoClient mongoClient) {
            this.mongoClient = mongoClient;
            this.closeMongoClientOnStop = false;
            return this;
        }

        public Builder closeMongoClientOnStop(boolean closeMongoClientOnStop) {
            this.closeMongoClientOnStop = closeMongoClientOnStop;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder collection(String collection) {
            this.collection = collection;
            return this;
        }

        public Builder captureMode(CaptureMode captureMode) {
            this.captureMode = captureMode == null ? CaptureMode.AUTO : captureMode;
            return this;
        }

        public Builder mongoVersion(String mongoVersion) {
            this.mongoVersion = mongoVersion == null ? null : MongoVersion.parse(mongoVersion);
            return this;
        }

        public Builder mongoVersion(MongoVersion mongoVersion) {
            this.mongoVersion = mongoVersion;
            return this;
        }

        public Builder fullDocument(FullDocumentMode fullDocument) {
            this.fullDocument = fullDocument;
            return this;
        }

        public Builder enablePreImage(boolean enablePreImage) {
            this.enablePreImage = enablePreImage;
            return this;
        }

        public Builder pipeline(List<BsonDocument> pipeline) {
            this.pipeline = pipeline;
            return this;
        }

        public Builder retryMaxTimes(int retryMaxTimes) {
            this.retryMaxTimes = retryMaxTimes;
            return this;
        }

        public Builder retryIntervalMs(long retryIntervalMs) {
            this.retryIntervalMs = retryIntervalMs;
            return this;
        }

        /**
         * 同步模式：{@link SyncMode#FULL} / {@link SyncMode#FULL_AND_INCREMENTAL} /
         * {@link SyncMode#FULL_AND_CATCH_UP} / {@link SyncMode#INCREMENTAL}。
         */
        public Builder syncMode(SyncMode syncMode) {
            this.syncMode = syncMode == null ? SyncMode.INCREMENTAL : syncMode;
            return this;
        }

        /**
         * ChangeStream 位点存储（ResumeToken）。仅 CHANGE_STREAM 模式使用。
         */
        public Builder resumeTokenStorage(ResumeTokenStorage resumeTokenStorage) {
            this.resumeTokenStorage = resumeTokenStorage;
            return this;
        }

        /**
         * Oplog 位点存储（时间戳）。仅 OPLOG 模式使用。
         */
        public Builder oplogOffsetStorage(OplogOffsetStorage oplogOffsetStorage) {
            this.oplogOffsetStorage = oplogOffsetStorage;
            return this;
        }

        public Builder listener(TransferEventListener listener) {
            this.listener = listener;
            return this;
        }

        public Builder ddlListener(DdlEventListener ddlListener) {
            this.ddlListener = ddlListener;
            return this;
        }

        public Builder listenerThreadPriority(int listenerThreadPriority) {
            this.listenerThreadPriority = listenerThreadPriority;
            return this;
        }

        public Builder oplogStartTimestamp(BsonTimestamp oplogStartTimestamp) {
            this.oplogStartTimestamp = oplogStartTimestamp;
            return this;
        }

        /** 增量结束 ts（含），对齐 d2t endOplogTime；0/null 表示不设上限。 */
        public Builder oplogEndTimestamp(BsonTimestamp oplogEndTimestamp) {
            this.oplogEndTimestamp = oplogEndTimestamp;
            return this;
        }

        public Builder oplogBatchSize(int oplogBatchSize) {
            this.oplogBatchSize = oplogBatchSize > 0 ? oplogBatchSize : DEFAULT_OPLOG_BATCH_SIZE;
            return this;
        }

        public Builder includeFromMigrate(boolean includeFromMigrate) {
            this.includeFromMigrate = includeFromMigrate;
            return this;
        }

        /**
         * 全量扫描结束后执行（全量+增量并行时增量可能已在写；用于尽力排空/flush）。
         */
        public Builder afterFullSyncBarrier(Runnable afterFullSyncBarrier) {
            this.afterFullSyncBarrier = afterFullSyncBarrier;
            return this;
        }

        /**
         * 周期性把当前位点打到 stderr 的间隔（秒）。默认 {@link #DEFAULT_OFFSET_LOG_INTERVAL_SECONDS}；
         * {@code <=0} 关闭。便于异常时从日志看到上次同步时间。
         */
        public Builder offsetLogIntervalSeconds(int offsetLogIntervalSeconds) {
            this.offsetLogIntervalSeconds = offsetLogIntervalSeconds;
            return this;
        }

        /**
         * 全量并行读线程数（对齐 d2t sourceThreadNum）。{@code <=1} 单游标；{@code >1} 按 _id 切段并行。
         */
        public Builder fullSyncParallelism(int fullSyncParallelism) {
            this.fullSyncParallelism = fullSyncParallelism > 0
                    ? fullSyncParallelism : DEFAULT_FULL_SYNC_PARALLELISM;
            return this;
        }

        /** 全量游标 batchSize。 */
        public Builder fullSyncBatchSize(int fullSyncBatchSize) {
            this.fullSyncBatchSize = fullSyncBatchSize > 0
                    ? fullSyncBatchSize : DEFAULT_FULL_SYNC_BATCH_SIZE;
            return this;
        }

        /**
         * 单段全量任务目标数据量（MB），对齐 d2t SpliceNsData.mbSize；用于估算 skip 切段文档数。
         */
        public Builder fullSyncTaskMbSize(int fullSyncTaskMbSize) {
            this.fullSyncTaskMbSize = fullSyncTaskMbSize > 0
                    ? fullSyncTaskMbSize : DEFAULT_FULL_SYNC_TASK_MB_SIZE;
            return this;
        }

        public MongoSourceConfig build() {
            if (mongoClient == null && (uri == null || uri.trim().isEmpty())) {
                throw new IllegalArgumentException("uri or mongoClient is required");
            }
            if (database == null || database.trim().isEmpty()) {
                throw new IllegalArgumentException("database is required");
            }
            if (collection == null || collection.trim().isEmpty()) {
                throw new IllegalArgumentException("collection is required");
            }
            if (listener == null) {
                throw new IllegalArgumentException("listener is required");
            }

            if (captureMode == CaptureMode.OPLOG) {
                // mongoVersion 可省略：Sync/Source 启动时用 buildInfo 自动探测
                if (mongoVersion != null && !mongoVersion.supportsOplog()) {
                    throw new IllegalArgumentException(
                            "MongoDB " + mongoVersion.getRaw()
                                    + " (7.0+) does not support OPLOG; use captureMode=CHANGE_STREAM or AUTO");
                }
                if (resumeTokenStorage != null && oplogOffsetStorage == null) {
                    throw new IllegalArgumentException(
                            "OPLOG mode requires oplogOffsetStorage (timestamp), not resumeTokenStorage");
                }
            } else if (captureMode == CaptureMode.CHANGE_STREAM) {
                if (oplogOffsetStorage != null && resumeTokenStorage == null) {
                    throw new IllegalArgumentException(
                            "CHANGE_STREAM mode requires resumeTokenStorage (ResumeToken), not oplogOffsetStorage");
                }
            }
            // AUTO：位点类型在解析为具体模式后再校验（由 Sync/Source 装配）

            MongoSourceConfig config = new MongoSourceConfig();
            config.uri = this.uri;
            config.mongoClient = this.mongoClient;
            config.closeMongoClientOnStop = this.mongoClient == null || this.closeMongoClientOnStop;
            config.database = this.database;
            config.collection = this.collection;
            config.captureMode = this.captureMode;
            config.mongoVersion = this.mongoVersion;
            config.oplogFormatVersion = this.mongoVersion != null && this.mongoVersion.supportsOplog()
                    ? this.mongoVersion.toOplogFormat() : null;
            config.fullDocument = this.fullDocument;
            config.enablePreImage = this.enablePreImage;
            config.pipeline = this.pipeline;
            config.retryMaxTimes = this.retryMaxTimes;
            config.retryIntervalMs = this.retryIntervalMs;
            config.syncMode = this.syncMode == null ? SyncMode.INCREMENTAL : this.syncMode;
            config.resumeTokenStorage = this.resumeTokenStorage;
            config.oplogOffsetStorage = this.oplogOffsetStorage;
            config.listener = this.listener;
            config.ddlListener = this.ddlListener;
            config.listenerThreadPriority = this.listenerThreadPriority;
            config.oplogStartTimestamp = this.oplogStartTimestamp;
            config.oplogEndTimestamp = this.oplogEndTimestamp;
            config.oplogBatchSize = this.oplogBatchSize;
            config.includeFromMigrate = this.includeFromMigrate;
            config.afterFullSyncBarrier = this.afterFullSyncBarrier;
            config.offsetLogIntervalSeconds = this.offsetLogIntervalSeconds;
            config.fullSyncParallelism = this.fullSyncParallelism;
            config.fullSyncBatchSize = this.fullSyncBatchSize;
            config.fullSyncTaskMbSize = this.fullSyncTaskMbSize;
            return config;
        }
    }
}
