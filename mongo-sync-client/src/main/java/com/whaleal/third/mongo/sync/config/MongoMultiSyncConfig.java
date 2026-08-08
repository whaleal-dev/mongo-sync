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
import com.whaleal.third.mongo.sync.ns.NamespaceFilter;
import com.whaleal.third.mongo.sync.ns.NamespaceMapper;
import com.whaleal.third.mongo.sync.spi.SyncWriteErrorHandler;

/**
 * 多库表同步配置：白/黑名单发现集合后，为每张表启动一个 {@link com.whaleal.third.mongo.sync.sdk.MongoSyncClient}。
 */
public final class MongoMultiSyncConfig {

    private String sourceUri;
    private String targetUri;
    private MongoClient sourceMongoClient;
    private MongoClient targetMongoClient;

    private String namespaceWhite;
    private String namespaceBlack;
    private String namespaceTransform;

    private CaptureMode captureMode = CaptureMode.AUTO;
    private MongoVersion mongoVersion;
    private MongoSourceConfig.FullDocumentMode fullDocument = MongoSourceConfig.FullDocumentMode.DEFAULT;
    private boolean enablePreImage;
    private SyncMode syncMode = SyncMode.FULL_AND_INCREMENTAL;
    private boolean includeFromMigrate;

    private WriteMode writeMode = WriteMode.UPSERT;
    private OnConflict onConflict = OnConflict.FAIL;
    private int targetBatchSize = 1000;
    private int targetWriterThreads = MongoSinkConfig.DEFAULT_WRITER_THREADS;

    private int bucketNum = MongoSyncConfig.DEFAULT_BUCKET_NUM;
    private int bucketQueueCapacity = MongoSyncConfig.DEFAULT_BUCKET_QUEUE_CAPACITY;
    private int ddlWaitSeconds = MongoSyncConfig.DEFAULT_DDL_WAIT_SECONDS;
    private boolean forceSingleBucketOnUniqueIndex = true;
    private SyncWriteErrorHandler writeErrorHandler;

    private boolean bootstrapCollection = true;
    private boolean bootstrapIndexes = true;
    private boolean skipTtlIndexes = true;
    private int offsetLogIntervalSeconds =
            MongoSourceConfig.DEFAULT_OFFSET_LOG_INTERVAL_SECONDS;

    /** 位点文件目录；非空则按 ns 持久化 ResumeToken / OplogOffset。 */
    private String offsetStoreDir;
    private int fullSyncParallelism = MongoSourceConfig.DEFAULT_FULL_SYNC_PARALLELISM;
    private int fullSyncBatchSize = MongoSourceConfig.DEFAULT_FULL_SYNC_BATCH_SIZE;
    private int fullSyncTaskMbSize = MongoSourceConfig.DEFAULT_FULL_SYNC_TASK_MB_SIZE;

    private long commitMaxLagMs = MongoSyncConfig.DEFAULT_COMMIT_MAX_LAG_MS;

    private MongoMultiSyncConfig() {
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

    public String getNamespaceWhite() {
        return namespaceWhite;
    }

    public String getNamespaceBlack() {
        return namespaceBlack;
    }

    public String getNamespaceTransform() {
        return namespaceTransform;
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
        return syncMode == null ? SyncMode.FULL_AND_INCREMENTAL : syncMode;
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

    public boolean isForceSingleBucketOnUniqueIndex() {
        return forceSingleBucketOnUniqueIndex;
    }

    public SyncWriteErrorHandler getWriteErrorHandler() {
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

    public NamespaceFilter namespaceFilter() {
        return NamespaceFilter.of(namespaceWhite, namespaceBlack);
    }

    public NamespaceMapper namespaceMapper() {
        return NamespaceMapper.of(namespaceTransform);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final MongoMultiSyncConfig c = new MongoMultiSyncConfig();

        public Builder sourceUri(String sourceUri) {
            c.sourceUri = sourceUri;
            return this;
        }

        public Builder targetUri(String targetUri) {
            c.targetUri = targetUri;
            return this;
        }

        public Builder sourceMongoClient(MongoClient client) {
            c.sourceMongoClient = client;
            return this;
        }

        public Builder targetMongoClient(MongoClient client) {
            c.targetMongoClient = client;
            return this;
        }

        /**
         * 白名单，分号分隔：{@code db1;db2.coll}（与黑名单互斥）。
         */
        public Builder namespaceWhite(String namespaceWhite) {
            c.namespaceWhite = namespaceWhite;
            return this;
        }

        /** 黑名单，分号分隔（与白名单互斥）。 */
        public Builder namespaceBlack(String namespaceBlack) {
            c.namespaceBlack = namespaceBlack;
            return this;
        }

        /**
         * ns 变换：{@code srcDb.srcColl:tgtDb.tgtColl;...}
         */
        public Builder namespaceTransform(String namespaceTransform) {
            c.namespaceTransform = namespaceTransform;
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

        public Builder fullDocument(MongoSourceConfig.FullDocumentMode fullDocument) {
            c.fullDocument = fullDocument;
            return this;
        }

        public Builder enablePreImage(boolean enablePreImage) {
            c.enablePreImage = enablePreImage;
            return this;
        }

        public Builder syncMode(SyncMode syncMode) {
            c.syncMode = syncMode == null ? SyncMode.FULL_AND_INCREMENTAL : syncMode;
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

        public Builder onConflict(OnConflict onConflict) {
            c.onConflict = onConflict == null ? OnConflict.FAIL : onConflict;
            return this;
        }

        public Builder targetBatchSize(int targetBatchSize) {
            c.targetBatchSize = targetBatchSize > 0 ? targetBatchSize : 1000;
            return this;
        }

        public Builder targetWriterThreads(int targetWriterThreads) {
            c.targetWriterThreads = targetWriterThreads > 0
                    ? targetWriterThreads
                    : MongoSinkConfig.DEFAULT_WRITER_THREADS;
            return this;
        }

        public Builder bucketNum(int bucketNum) {
            c.bucketNum = bucketNum > 0 ? bucketNum : MongoSyncConfig.DEFAULT_BUCKET_NUM;
            return this;
        }

        public Builder bucketQueueCapacity(int bucketQueueCapacity) {
            c.bucketQueueCapacity = bucketQueueCapacity > 0
                    ? bucketQueueCapacity
                    : MongoSyncConfig.DEFAULT_BUCKET_QUEUE_CAPACITY;
            return this;
        }

        public Builder ddlWaitSeconds(int ddlWaitSeconds) {
            c.ddlWaitSeconds = ddlWaitSeconds > 0
                    ? ddlWaitSeconds
                    : MongoSyncConfig.DEFAULT_DDL_WAIT_SECONDS;
            return this;
        }

        public Builder forceSingleBucketOnUniqueIndex(boolean force) {
            c.forceSingleBucketOnUniqueIndex = force;
            return this;
        }

        public Builder writeErrorHandler(SyncWriteErrorHandler writeErrorHandler) {
            c.writeErrorHandler = writeErrorHandler;
            return this;
        }

        public Builder bootstrapCollection(boolean bootstrapCollection) {
            c.bootstrapCollection = bootstrapCollection;
            return this;
        }

        public Builder bootstrapIndexes(boolean bootstrapIndexes) {
            c.bootstrapIndexes = bootstrapIndexes;
            return this;
        }

        public Builder skipTtlIndexes(boolean skipTtlIndexes) {
            c.skipTtlIndexes = skipTtlIndexes;
            return this;
        }

        public Builder offsetLogIntervalSeconds(int offsetLogIntervalSeconds) {
            c.offsetLogIntervalSeconds = offsetLogIntervalSeconds;
            return this;
        }

        /**
         * 位点持久化目录。设置后每张表使用独立文件保存 ResumeToken / OplogOffset。
         */
        public Builder offsetStoreDir(String offsetStoreDir) {
            c.offsetStoreDir = offsetStoreDir;
            return this;
        }

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

        public Builder fullSyncTaskMbSize(int fullSyncTaskMbSize) {
            c.fullSyncTaskMbSize = fullSyncTaskMbSize > 0
                    ? fullSyncTaskMbSize : MongoSourceConfig.DEFAULT_FULL_SYNC_TASK_MB_SIZE;
            return this;
        }

        public Builder commitMaxLagMs(long commitMaxLagMs) {
            c.commitMaxLagMs = commitMaxLagMs > 0L
                    ? commitMaxLagMs : MongoSyncConfig.DEFAULT_COMMIT_MAX_LAG_MS;
            return this;
        }

        public MongoMultiSyncConfig build() {
            if (c.sourceMongoClient == null && (c.sourceUri == null || c.sourceUri.trim().isEmpty())) {
                throw new MongoSyncException(MongoSyncErrorCode.CONFIG_REQUIRED,
                        "sourceUri or sourceMongoClient is required");
            }
            if (c.targetMongoClient == null && (c.targetUri == null || c.targetUri.trim().isEmpty())) {
                throw new MongoSyncException(MongoSyncErrorCode.CONFIG_REQUIRED,
                        "targetUri or targetMongoClient is required");
            }
            c.namespaceFilter();
            c.namespaceMapper();
            return c;
        }
    }
}
