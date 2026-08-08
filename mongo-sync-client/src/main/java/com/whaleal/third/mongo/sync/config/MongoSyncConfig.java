package com.whaleal.third.mongo.sync.config;

import com.mongodb.client.MongoClient;
import com.whaleal.third.mongo.sink.config.MongoSinkConfig;
import com.whaleal.third.mongo.sink.config.OnConflict;
import com.whaleal.third.mongo.sink.config.WriteMode;
import com.whaleal.third.mongo.source.config.CaptureMode;
import com.whaleal.third.mongo.source.config.MongoSourceConfig;
import com.whaleal.third.mongo.source.config.SyncMode;
import com.whaleal.third.mongo.sync.error.MongoSyncErrorCode;
import com.whaleal.third.mongo.sync.error.MongoSyncException;
import com.whaleal.third.mongo.source.oplog.MongoVersion;
import com.whaleal.third.mongo.source.spi.OplogOffsetStorage;
import com.whaleal.third.mongo.source.spi.ResumeTokenStorage;
import org.bson.BsonTimestamp;

/**
 * 文档库同步配置：统一用 source / target 表示两端（模块名仍为 mongo-sink-client）。
 */
public class MongoSyncConfig {

    public static final int DEFAULT_BUCKET_NUM = 16;
    public static final int DEFAULT_BUCKET_QUEUE_CAPACITY = 8192; // Disruptor RingBuffer，须为 2 的幂
    public static final int DEFAULT_DDL_WAIT_SECONDS = 30;
    public static final long DEFAULT_NS_LOCK_EXPIRE_MINUTES = 30;

    public static final long DEFAULT_COMMIT_MAX_LAG_MS = 10_000L;

    private String sourceUri;
    private String targetUri;
    private MongoClient sourceMongoClient;
    private MongoClient targetMongoClient;
    private boolean closeSourceClientOnStop = true;
    private boolean closeTargetClientOnClose = true;

    private String sourceDatabase;
    private String sourceCollection;
    private String targetDatabase;
    private String targetCollection;

    /** 默认 AUTO：按源端 standalone / replicaSet / sharding 自动匹配读任务。 */
    private CaptureMode captureMode = CaptureMode.AUTO;
    private MongoVersion mongoVersion;
    private MongoSourceConfig.FullDocumentMode fullDocument = MongoSourceConfig.FullDocumentMode.DEFAULT;
    private boolean enablePreImage;
    private SyncMode syncMode = SyncMode.INCREMENTAL;
    private ResumeTokenStorage resumeTokenStorage;
    private OplogOffsetStorage oplogOffsetStorage;
    private BsonTimestamp oplogStartTimestamp;
    private BsonTimestamp oplogEndTimestamp;
    private boolean includeFromMigrate;

    private WriteMode writeMode = WriteMode.UPSERT;
    private OnConflict onConflict = OnConflict.FAIL;
    private int targetBatchSize = 1000;
    private int targetWriterThreads = MongoSinkConfig.DEFAULT_WRITER_THREADS;

    private int bucketNum = DEFAULT_BUCKET_NUM;
    private int bucketQueueCapacity = DEFAULT_BUCKET_QUEUE_CAPACITY;
    private int ddlWaitSeconds = DEFAULT_DDL_WAIT_SECONDS;
    private long nsLockExpireMinutes = DEFAULT_NS_LOCK_EXPIRE_MINUTES;
    private boolean forceSingleBucketOnUniqueIndex = true;
    private com.whaleal.third.mongo.sync.spi.SyncWriteErrorHandler writeErrorHandler;

    /** 启动时从源端拉取集合定义并在目标创建表，默认开启。 */
    private boolean bootstrapCollection = true;
    /** 启动时是否在目标创建源端非 _id 索引，默认开启。 */
    private boolean bootstrapIndexes = true;
    /** 建索引时是否跳过 TTL（expireAfterSeconds），默认 true（对齐 d2t）。 */
    private boolean skipTtlIndexes = true;
    /** 周期性打印位点间隔（秒），透传 Source；默认 30，{@code <=0} 关闭。 */
    private int offsetLogIntervalSeconds = com.whaleal.third.mongo.source.config.MongoSourceConfig.DEFAULT_OFFSET_LOG_INTERVAL_SECONDS;
    /**
     * 位点文件目录；非空则该表使用文件持久化 ResumeToken / OplogOffset。
     * 多表场景请用 {@link MongoMultiSyncConfig#getOffsetStoreDir()}。
     */
    private String offsetStoreDir;
    /** 全量并行读线程数，透传 Source；默认 1。 */
    private int fullSyncParallelism = MongoSourceConfig.DEFAULT_FULL_SYNC_PARALLELISM;
    private int fullSyncBatchSize = MongoSourceConfig.DEFAULT_FULL_SYNC_BATCH_SIZE;
    /** 单段全量任务目标体积（MB），默认 32。 */
    private int fullSyncTaskMbSize = MongoSourceConfig.DEFAULT_FULL_SYNC_TASK_MB_SIZE;

    /** 允许 commit 的最大增量滞后（毫秒）；仅含增量模式生效。 */
    private long commitMaxLagMs = DEFAULT_COMMIT_MAX_LAG_MS;

    private MongoSyncConfig() {
    }

    public String getSourceUri() {
        return sourceUri;
    }

    public String getTargetUri() {
        return targetUri;
    }

    public MongoClient getSourceMongoClient() {
        return sourceMongoClient;
    }

    public MongoClient getTargetMongoClient() {
        return targetMongoClient;
    }

    public boolean isCloseSourceClientOnStop() {
        return closeSourceClientOnStop;
    }

    public boolean isCloseTargetClientOnClose() {
        return closeTargetClientOnClose;
    }

    public String getSourceDatabase() {
        return sourceDatabase;
    }

    public String getSourceCollection() {
        return sourceCollection;
    }

    public String getTargetDatabase() {
        return targetDatabase;
    }

    public String getTargetCollection() {
        return targetCollection;
    }

    public CaptureMode getCaptureMode() {
        return captureMode;
    }

    public MongoVersion getMongoVersion() {
        return mongoVersion;
    }

    public MongoSourceConfig.FullDocumentMode getFullDocument() {
        return fullDocument;
    }

    public boolean isEnablePreImage() {
        return enablePreImage;
    }

    public SyncMode getSyncMode() {
        return syncMode == null ? SyncMode.INCREMENTAL : syncMode;
    }

    public ResumeTokenStorage getResumeTokenStorage() {
        return resumeTokenStorage;
    }

    public OplogOffsetStorage getOplogOffsetStorage() {
        return oplogOffsetStorage;
    }

    public BsonTimestamp getOplogStartTimestamp() {
        return oplogStartTimestamp;
    }

    public BsonTimestamp getOplogEndTimestamp() {
        return oplogEndTimestamp;
    }

    public boolean isIncludeFromMigrate() {
        return includeFromMigrate;
    }

    public WriteMode getWriteMode() {
        return writeMode;
    }

    public OnConflict getOnConflict() {
        return onConflict;
    }

    public int getTargetBatchSize() {
        return targetBatchSize;
    }

    public int getTargetWriterThreads() {
        return targetWriterThreads;
    }

    public int getBucketNum() {
        return bucketNum;
    }

    public int getBucketQueueCapacity() {
        return bucketQueueCapacity;
    }

    public int getDdlWaitSeconds() {
        return ddlWaitSeconds;
    }

    public long getNsLockExpireMinutes() {
        return nsLockExpireMinutes;
    }

    public boolean isForceSingleBucketOnUniqueIndex() {
        return forceSingleBucketOnUniqueIndex;
    }

    public com.whaleal.third.mongo.sync.spi.SyncWriteErrorHandler getWriteErrorHandler() {
        return writeErrorHandler;
    }

    public boolean isBootstrapCollection() {
        return bootstrapCollection;
    }

    public boolean isBootstrapIndexes() {
        return bootstrapIndexes;
    }

    public boolean isSkipTtlIndexes() {
        return skipTtlIndexes;
    }

    public int getOffsetLogIntervalSeconds() {
        return offsetLogIntervalSeconds;
    }

    public String getOffsetStoreDir() {
        return offsetStoreDir;
    }

    public int getFullSyncParallelism() {
        return fullSyncParallelism;
    }

    public int getFullSyncBatchSize() {
        return fullSyncBatchSize;
    }

    public int getFullSyncTaskMbSize() {
        return fullSyncTaskMbSize;
    }

    public long getCommitMaxLagMs() {
        return commitMaxLagMs;
    }

    public String sourceNs() {
        return sourceDatabase + "." + sourceCollection;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final MongoSyncConfig c = new MongoSyncConfig();

        public Builder sourceUri(String sourceUri) {
            c.sourceUri = sourceUri;
            return this;
        }

        public Builder targetUri(String targetUri) {
            c.targetUri = targetUri;
            return this;
        }

        public Builder sourceMongoClient(MongoClient sourceMongoClient) {
            c.sourceMongoClient = sourceMongoClient;
            c.closeSourceClientOnStop = false;
            return this;
        }

        public Builder targetMongoClient(MongoClient targetMongoClient) {
            c.targetMongoClient = targetMongoClient;
            c.closeTargetClientOnClose = false;
            return this;
        }

        public Builder closeSourceClientOnStop(boolean close) {
            c.closeSourceClientOnStop = close;
            return this;
        }

        public Builder closeTargetClientOnClose(boolean close) {
            c.closeTargetClientOnClose = close;
            return this;
        }

        public Builder sourceDatabase(String sourceDatabase) {
            c.sourceDatabase = sourceDatabase;
            return this;
        }

        public Builder sourceCollection(String sourceCollection) {
            c.sourceCollection = sourceCollection;
            return this;
        }

        public Builder targetDatabase(String targetDatabase) {
            c.targetDatabase = targetDatabase;
            return this;
        }

        public Builder targetCollection(String targetCollection) {
            c.targetCollection = targetCollection;
            return this;
        }

        /** 同源同名映射到目标。 */
        public Builder mapCollection(String database, String collection) {
            c.sourceDatabase = database;
            c.sourceCollection = collection;
            c.targetDatabase = database;
            c.targetCollection = collection;
            return this;
        }

        public Builder captureMode(CaptureMode captureMode) {
            c.captureMode = captureMode == null ? CaptureMode.AUTO : captureMode;
            return this;
        }

        public Builder mongoVersion(String mongoVersion) {
            c.mongoVersion = mongoVersion == null ? null : MongoVersion.parse(mongoVersion);
            return this;
        }

        public Builder mongoVersion(MongoVersion mongoVersion) {
            c.mongoVersion = mongoVersion;
            return this;
        }

        public Builder fullDocument(MongoSourceConfig.FullDocumentMode fullDocument) {
            c.fullDocument = fullDocument;
            return this;
        }

        public Builder enablePreImage(boolean enablePreImage) {
            c.enablePreImage = enablePreImage;
            return this;
        }

        /**
         * 同步模式：{@link SyncMode#FULL} / {@link SyncMode#FULL_AND_INCREMENTAL} /
         * {@link SyncMode#FULL_AND_CATCH_UP} / {@link SyncMode#INCREMENTAL}。
         */
        public Builder syncMode(SyncMode syncMode) {
            c.syncMode = syncMode == null ? SyncMode.INCREMENTAL : syncMode;
            return this;
        }

        public Builder resumeTokenStorage(ResumeTokenStorage resumeTokenStorage) {
            c.resumeTokenStorage = resumeTokenStorage;
            return this;
        }

        public Builder oplogOffsetStorage(OplogOffsetStorage oplogOffsetStorage) {
            c.oplogOffsetStorage = oplogOffsetStorage;
            return this;
        }

        public Builder oplogStartTimestamp(BsonTimestamp oplogStartTimestamp) {
            c.oplogStartTimestamp = oplogStartTimestamp;
            return this;
        }

        /** 增量结束 ts（含）；{@link SyncMode#FULL_AND_CATCH_UP} 也可由全量结束自动填写。 */
        public Builder oplogEndTimestamp(BsonTimestamp oplogEndTimestamp) {
            c.oplogEndTimestamp = oplogEndTimestamp;
            return this;
        }

        public Builder includeFromMigrate(boolean includeFromMigrate) {
            c.includeFromMigrate = includeFromMigrate;
            return this;
        }

        public Builder writeMode(WriteMode writeMode) {
            c.writeMode = writeMode == null ? WriteMode.UPSERT : writeMode;
            return this;
        }

        /**
         * 主键/唯一键冲突：FAIL / SKIP（跳过+日志）/ UPSERT（转为 upsert）。
         */
        public Builder onConflict(OnConflict onConflict) {
            c.onConflict = onConflict == null ? OnConflict.FAIL : onConflict;
            return this;
        }

        /** 目标端 bulk 批量大小（写入 MongoSinkClient）。 */
        public Builder targetBatchSize(int targetBatchSize) {
            c.targetBatchSize = targetBatchSize > 0 ? targetBatchSize : 1000;
            return this;
        }

        /**
         * 目标端写线程数，默认 {@link MongoSinkConfig#DEFAULT_WRITER_THREADS}（8）。
         * 不同 ns 可并发写入；同 ns 同 _id 由分桶 Disruptor + flush 保序。
         */
        public Builder targetWriterThreads(int targetWriterThreads) {
            c.targetWriterThreads = targetWriterThreads > 0
                    ? targetWriterThreads
                    : MongoSinkConfig.DEFAULT_WRITER_THREADS;
            return this;
        }

        /** 分桶数，对齐 d2t maxBucketNum；同 _id 进同桶保证有序。 */
        public Builder bucketNum(int bucketNum) {
            c.bucketNum = bucketNum > 0 ? bucketNum : DEFAULT_BUCKET_NUM;
            return this;
        }

        /** Disruptor RingBuffer 容量（自动向上取整为 2 的幂）。 */
        public Builder bucketQueueCapacity(int bucketQueueCapacity) {
            c.bucketQueueCapacity = bucketQueueCapacity > 0 ? bucketQueueCapacity : DEFAULT_BUCKET_QUEUE_CAPACITY;
            return this;
        }

        /** DDL 前等待在途 CRUD 排空的超时秒数。 */
        public Builder ddlWaitSeconds(int ddlWaitSeconds) {
            c.ddlWaitSeconds = ddlWaitSeconds > 0 ? ddlWaitSeconds : DEFAULT_DDL_WAIT_SECONDS;
            return this;
        }

        public Builder nsLockExpireMinutes(long nsLockExpireMinutes) {
            c.nsLockExpireMinutes = nsLockExpireMinutes > 0 ? nsLockExpireMinutes : DEFAULT_NS_LOCK_EXPIRE_MINUTES;
            return this;
        }

        public Builder forceSingleBucketOnUniqueIndex(boolean force) {
            c.forceSingleBucketOnUniqueIndex = force;
            return this;
        }

        public Builder writeErrorHandler(com.whaleal.third.mongo.sync.spi.SyncWriteErrorHandler writeErrorHandler) {
            c.writeErrorHandler = writeErrorHandler;
            return this;
        }

        /**
         * 同步开始前是否从源端获取集合定义并在目标创建集合/视图。默认 {@code true}。
         * 索引创建由 {@link #bootstrapIndexes(boolean)} 单独控制。
         */
        public Builder bootstrapCollection(boolean bootstrapCollection) {
            c.bootstrapCollection = bootstrapCollection;
            return this;
        }

        /**
         * 同步开始前是否在目标创建源端非 {@code _id_} 索引。默认 {@code true}。
         * 需目标集合已存在，或同时开启 {@link #bootstrapCollection(boolean)}。
         */
        public Builder bootstrapIndexes(boolean bootstrapIndexes) {
            c.bootstrapIndexes = bootstrapIndexes;
            return this;
        }

        /**
         * 预建索引时是否跳过 TTL 索引。默认 {@code true}（对齐 d2t）。
         * 仅在 {@link #bootstrapIndexes(boolean)} 为 true 时生效。
         */
        public Builder skipTtlIndexes(boolean skipTtlIndexes) {
            c.skipTtlIndexes = skipTtlIndexes;
            return this;
        }

        /**
         * 周期性把当前位点打到 stderr 的间隔（秒）。默认 30；{@code <=0} 关闭。
         * 异常时可从日志看到上次同步时间（oplog ts / clusterTime）。
         */
        public Builder offsetLogIntervalSeconds(int offsetLogIntervalSeconds) {
            c.offsetLogIntervalSeconds = offsetLogIntervalSeconds;
            return this;
        }

        /**
         * 位点持久化目录。设置后为本表创建文件存储（ResumeToken 或 OplogOffset）。
         */
        public Builder offsetStoreDir(String offsetStoreDir) {
            c.offsetStoreDir = offsetStoreDir;
            return this;
        }

        /** 全量并行读线程数（对齐 d2t sourceThreadNum）；>1 按 _id 切段并行。 */
        public Builder fullSyncParallelism(int fullSyncParallelism) {
            c.fullSyncParallelism = fullSyncParallelism > 0
                    ? fullSyncParallelism : MongoSourceConfig.DEFAULT_FULL_SYNC_PARALLELISM;
            return this;
        }

        public Builder fullSyncBatchSize(int fullSyncBatchSize) {
            c.fullSyncBatchSize = fullSyncBatchSize > 0
                    ? fullSyncBatchSize : MongoSourceConfig.DEFAULT_FULL_SYNC_BATCH_SIZE;
            return this;
        }

        /** 单段全量任务目标体积（MB），默认 32。 */
        public Builder fullSyncTaskMbSize(int fullSyncTaskMbSize) {
            c.fullSyncTaskMbSize = fullSyncTaskMbSize > 0
                    ? fullSyncTaskMbSize : MongoSourceConfig.DEFAULT_FULL_SYNC_TASK_MB_SIZE;
            return this;
        }

        /**
         * 允许 commit 的最大增量滞后（毫秒）。默认 10000。
         * 仅 {@link SyncMode#includeIncremental()} 时参与 {@code canCommit} 判定。
         */
        public Builder commitMaxLagMs(long commitMaxLagMs) {
            c.commitMaxLagMs = commitMaxLagMs > 0L
                    ? commitMaxLagMs : DEFAULT_COMMIT_MAX_LAG_MS;
            return this;
        }

        public MongoSyncConfig build() {
            if (c.sourceMongoClient == null && (c.sourceUri == null || c.sourceUri.trim().isEmpty())) {
                throw new MongoSyncException(MongoSyncErrorCode.CONFIG_REQUIRED,
                        "sourceUri or sourceMongoClient is required");
            }
            if (c.targetMongoClient == null && (c.targetUri == null || c.targetUri.trim().isEmpty())) {
                throw new MongoSyncException(MongoSyncErrorCode.CONFIG_REQUIRED,
                        "targetUri or targetMongoClient is required");
            }
            if (blank(c.sourceDatabase) || blank(c.sourceCollection)) {
                throw new MongoSyncException(MongoSyncErrorCode.CONFIG_REQUIRED,
                        "source database/collection is required");
            }
            if (blank(c.targetDatabase) || blank(c.targetCollection)) {
                throw new MongoSyncException(MongoSyncErrorCode.CONFIG_REQUIRED,
                        "target database/collection is required");
            }
            return c;
        }

        private static boolean blank(String s) {
            return s == null || s.trim().isEmpty();
        }
    }
}
