package com.whaleal.third.mongo.sync.config;

import com.mongodb.client.MongoClient;
import com.whaleal.third.mongo.sink.config.MongoSinkConfig;
import com.whaleal.third.mongo.sink.config.OnConflict;
import com.whaleal.third.mongo.sink.config.WriteMode;
import com.whaleal.third.mongo.sink.kafka.config.KafkaOutputFormat;
import com.whaleal.third.mongo.sink.kafka.config.KafkaSinkConfig;
import com.whaleal.third.mongo.source.config.CaptureMode;
import com.whaleal.third.mongo.source.config.MongoSourceConfig;
import com.whaleal.third.mongo.source.config.SyncMode;
import com.whaleal.third.mongo.sync.error.MongoSyncErrorCode;
import com.whaleal.third.mongo.sync.error.MongoSyncException;
import com.whaleal.third.mongo.source.oplog.MongoVersion;
import com.whaleal.third.mongo.source.spi.OplogOffsetStorage;
import com.whaleal.third.mongo.source.spi.ResumeTokenStorage;
import org.bson.BsonTimestamp;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 文档库同步配置：统一用 source / sink 表示两端。
 * Sink 形态见 {@link SinkType}：MongoDB（默认）或 Kafka。
 */
public class MongoSyncConfig {

    public static final int DEFAULT_BUCKET_NUM = 16;
    public static final int DEFAULT_BUCKET_QUEUE_CAPACITY = 8192; // Disruptor RingBuffer，须为 2 的幂
    public static final int DEFAULT_DDL_WAIT_SECONDS = 30;
    public static final long DEFAULT_NS_LOCK_EXPIRE_MINUTES = 30;

    public static final long DEFAULT_COMMIT_MAX_LAG_MS = 10_000L;

    private String sourceUri;
    private String sinkUri;
    private MongoClient sourceMongoClient;
    private MongoClient sinkMongoClient;
    private boolean closeSourceClientOnStop = true;
    private boolean closeSinkClientOnClose = true;

    private String sourceDatabase;
    private String sourceCollection;
    private String sinkDatabase;
    private String sinkCollection;

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
    private int sinkBatchSize = 1000;
    private int sinkWriterThreads = MongoSinkConfig.DEFAULT_WRITER_THREADS;

    private int bucketNum = DEFAULT_BUCKET_NUM;
    private int bucketQueueCapacity = DEFAULT_BUCKET_QUEUE_CAPACITY;
    private int ddlWaitSeconds = DEFAULT_DDL_WAIT_SECONDS;
    private long nsLockExpireMinutes = DEFAULT_NS_LOCK_EXPIRE_MINUTES;
    private boolean forceSingleBucketOnUniqueIndex = true;
    private com.whaleal.third.mongo.sync.spi.SyncWriteErrorHandler writeErrorHandler;

    /** 启动时从源端拉取集合定义并在 Sink 端创建表，默认开启。 */
    private boolean bootstrapCollection = true;
    /** 启动时是否在 Sink 端创建源端非 _id 索引，默认开启。 */
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

    /**
     * 捕获窗口告警阈值（秒）。全量∥增量时监控锚定位点相对 oplog 最早条目的余量。
     * 默认 3600；{@code <=0} 关闭。
     */
    private int windowWarnSeconds = MongoSourceConfig.DEFAULT_WINDOW_WARN_SECONDS;

    /** 允许 commit 的最大增量滞后（毫秒）；仅含增量模式生效。 */
    private long commitMaxLagMs = DEFAULT_COMMIT_MAX_LAG_MS;

    /** Sink 形态，默认 MongoDB。 */
    private SinkType sinkType = SinkType.MONGODB;
    private String kafkaTopic;
    private String kafkaTopicPrefix = "";
    private String kafkaTopicSeparator = KafkaSinkConfig.DEFAULT_TOPIC_SEPARATOR;
    private String kafkaTopicSuffix = "";
    private KafkaOutputFormat kafkaOutputFormat = KafkaOutputFormat.JSON;
    private boolean kafkaPublishDdl = true;
    private String kafkaAcks = KafkaSinkConfig.DEFAULT_ACKS;
    private int kafkaLingerMs = KafkaSinkConfig.DEFAULT_LINGER_MS;
    private int kafkaBatchSizeBytes = KafkaSinkConfig.DEFAULT_BATCH_SIZE_BYTES;
    private String kafkaCompressionType = KafkaSinkConfig.DEFAULT_COMPRESSION;
    private String kafkaClientId = KafkaSinkConfig.DEFAULT_CLIENT_ID;
    private Map<String, String> kafkaProducerProperties = Collections.emptyMap();

    private MongoSyncConfig() {
    }

    public String getSourceUri() {
        return sourceUri;
    }

    public String getSinkUri() {
        return sinkUri;
    }

    public MongoClient getSourceMongoClient() {
        return sourceMongoClient;
    }

    public MongoClient getSinkMongoClient() {
        return sinkMongoClient;
    }

    public boolean isCloseSourceClientOnStop() {
        return closeSourceClientOnStop;
    }

    public boolean isCloseSinkClientOnClose() {
        return closeSinkClientOnClose;
    }

    public String getSourceDatabase() {
        return sourceDatabase;
    }

    public String getSourceCollection() {
        return sourceCollection;
    }

    public String getSinkDatabase() {
        return sinkDatabase;
    }

    public String getSinkCollection() {
        return sinkCollection;
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

    public int getSinkBatchSize() {
        return sinkBatchSize;
    }

    public int getSinkWriterThreads() {
        return sinkWriterThreads;
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

    public int getWindowWarnSeconds() {
        return windowWarnSeconds;
    }

    public long getCommitMaxLagMs() {
        return commitMaxLagMs;
    }

    public SinkType getSinkType() {
        return sinkType == null ? SinkType.MONGODB : sinkType;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public String getKafkaTopicPrefix() {
        return kafkaTopicPrefix;
    }

    public String getKafkaTopicSeparator() {
        return kafkaTopicSeparator;
    }

    public String getKafkaTopicSuffix() {
        return kafkaTopicSuffix;
    }

    public KafkaOutputFormat getKafkaOutputFormat() {
        return kafkaOutputFormat == null ? KafkaOutputFormat.JSON : kafkaOutputFormat;
    }

    public boolean isKafkaPublishDdl() {
        return kafkaPublishDdl;
    }

    public String getKafkaAcks() {
        return kafkaAcks;
    }

    public int getKafkaLingerMs() {
        return kafkaLingerMs;
    }

    public int getKafkaBatchSizeBytes() {
        return kafkaBatchSizeBytes;
    }

    public String getKafkaCompressionType() {
        return kafkaCompressionType;
    }

    public String getKafkaClientId() {
        return kafkaClientId;
    }

    public Map<String, String> getKafkaProducerProperties() {
        return kafkaProducerProperties;
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

        public Builder sinkUri(String sinkUri) {
            c.sinkUri = sinkUri;
            return this;
        }

        public Builder sourceMongoClient(MongoClient sourceMongoClient) {
            c.sourceMongoClient = sourceMongoClient;
            c.closeSourceClientOnStop = false;
            return this;
        }

        public Builder sinkMongoClient(MongoClient sinkMongoClient) {
            c.sinkMongoClient = sinkMongoClient;
            c.closeSinkClientOnClose = false;
            return this;
        }

        public Builder closeSourceClientOnStop(boolean close) {
            c.closeSourceClientOnStop = close;
            return this;
        }

        public Builder closeSinkClientOnClose(boolean close) {
            c.closeSinkClientOnClose = close;
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

        public Builder sinkDatabase(String sinkDatabase) {
            c.sinkDatabase = sinkDatabase;
            return this;
        }

        public Builder sinkCollection(String sinkCollection) {
            c.sinkCollection = sinkCollection;
            return this;
        }

        /** 同源同名映射到 Sink。 */
        public Builder mapCollection(String database, String collection) {
            c.sourceDatabase = database;
            c.sourceCollection = collection;
            c.sinkDatabase = database;
            c.sinkCollection = collection;
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
         * {@link SyncMode#FULL_THEN_CATCH_UP} / {@link SyncMode#INCREMENTAL}。
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

        /** 增量结束 ts（含）；{@link SyncMode#FULL_THEN_CATCH_UP} 也可由全量结束自动填写。 */
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

        /** Sink 端 bulk 批量大小（写入 MongoSinkClient）。 */
        public Builder sinkBatchSize(int sinkBatchSize) {
            c.sinkBatchSize = sinkBatchSize > 0 ? sinkBatchSize : 1000;
            return this;
        }

        /**
         * Sink 端写线程数，默认 {@link MongoSinkConfig#DEFAULT_WRITER_THREADS}（8）。
         * 不同 ns 可并发写入；同 ns 同 _id 由分桶 Disruptor + flush 保序。
         */
        public Builder sinkWriterThreads(int sinkWriterThreads) {
            c.sinkWriterThreads = sinkWriterThreads > 0
                    ? sinkWriterThreads
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
         * 同步开始前是否从源端获取集合定义并在 Sink 端创建集合/视图。默认 {@code true}。
         * 索引创建由 {@link #bootstrapIndexes(boolean)} 单独控制。
         */
        public Builder bootstrapCollection(boolean bootstrapCollection) {
            c.bootstrapCollection = bootstrapCollection;
            return this;
        }

        /**
         * 同步开始前是否在 Sink 端创建源端非 {@code _id_} 索引。默认 {@code true}。
         * 需 Sink 集合已存在，或同时开启 {@link #bootstrapCollection(boolean)}。
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
         * 捕获窗口告警阈值（秒）。默认 3600；{@code <=0} 关闭。
         * 全量∥增量期间：锚定位点 − oplog 最早条目。
         */
        public Builder windowWarnSeconds(int windowWarnSeconds) {
            c.windowWarnSeconds = windowWarnSeconds;
            return this;
        }

        /**
         * 允许 commit 的最大增量滞后（毫秒）。默认 10000。
         * 仅 {@link SyncMode#includesIncremental()} 时参与 {@code canCommit} 判定。
         */
        public Builder commitMaxLagMs(long commitMaxLagMs) {
            c.commitMaxLagMs = commitMaxLagMs > 0L
                    ? commitMaxLagMs : DEFAULT_COMMIT_MAX_LAG_MS;
            return this;
        }

        public Builder sinkType(SinkType sinkType) {
            c.sinkType = sinkType == null ? SinkType.MONGODB : sinkType;
            return this;
        }

        /** 固定 Kafka topic；不设则按 prefix + sinkDb + sep + sinkColl 拼接（对齐 mongo-kafka）。 */
        public Builder kafkaTopic(String kafkaTopic) {
            c.kafkaTopic = kafkaTopic;
            return this;
        }

        public Builder kafkaTopicPrefix(String kafkaTopicPrefix) {
            c.kafkaTopicPrefix = kafkaTopicPrefix == null ? "" : kafkaTopicPrefix;
            return this;
        }

        public Builder kafkaTopicSeparator(String kafkaTopicSeparator) {
            c.kafkaTopicSeparator = (kafkaTopicSeparator == null || kafkaTopicSeparator.isEmpty())
                    ? KafkaSinkConfig.DEFAULT_TOPIC_SEPARATOR : kafkaTopicSeparator;
            return this;
        }

        public Builder kafkaTopicSuffix(String kafkaTopicSuffix) {
            c.kafkaTopicSuffix = kafkaTopicSuffix == null ? "" : kafkaTopicSuffix;
            return this;
        }

        public Builder kafkaOutputFormat(KafkaOutputFormat kafkaOutputFormat) {
            c.kafkaOutputFormat = kafkaOutputFormat == null ? KafkaOutputFormat.JSON : kafkaOutputFormat;
            return this;
        }

        public Builder kafkaPublishDdl(boolean kafkaPublishDdl) {
            c.kafkaPublishDdl = kafkaPublishDdl;
            return this;
        }

        public Builder kafkaAcks(String kafkaAcks) {
            c.kafkaAcks = kafkaAcks;
            return this;
        }

        public Builder kafkaLingerMs(int kafkaLingerMs) {
            c.kafkaLingerMs = kafkaLingerMs;
            return this;
        }

        public Builder kafkaBatchSizeBytes(int kafkaBatchSizeBytes) {
            c.kafkaBatchSizeBytes = kafkaBatchSizeBytes;
            return this;
        }

        public Builder kafkaCompressionType(String kafkaCompressionType) {
            c.kafkaCompressionType = kafkaCompressionType;
            return this;
        }

        public Builder kafkaClientId(String kafkaClientId) {
            c.kafkaClientId = kafkaClientId;
            return this;
        }

        public Builder kafkaProducerProperties(Map<String, String> kafkaProducerProperties) {
            if (kafkaProducerProperties == null || kafkaProducerProperties.isEmpty()) {
                c.kafkaProducerProperties = Collections.emptyMap();
            } else {
                c.kafkaProducerProperties = Collections.unmodifiableMap(
                        new LinkedHashMap<String, String>(kafkaProducerProperties));
            }
            return this;
        }

        public MongoSyncConfig build() {
            if (c.sourceMongoClient == null && (c.sourceUri == null || c.sourceUri.trim().isEmpty())) {
                throw new MongoSyncException(MongoSyncErrorCode.CONFIG_REQUIRED,
                        "sourceUri or sourceMongoClient is required");
            }
            SinkType type = c.sinkType == null ? SinkType.MONGODB : c.sinkType;
            if (type == SinkType.KAFKA) {
                if (c.sinkUri == null || c.sinkUri.trim().isEmpty()) {
                    throw new MongoSyncException(MongoSyncErrorCode.CONFIG_REQUIRED,
                            "sinkUri (Kafka bootstrap servers) is required when sinkType=KAFKA");
                }
                if (c.sinkMongoClient != null) {
                    throw new MongoSyncException(MongoSyncErrorCode.CONFIG_INVALID,
                            "sinkMongoClient is not used when sinkType=KAFKA");
                }
            } else if (c.sinkMongoClient == null && (c.sinkUri == null || c.sinkUri.trim().isEmpty())) {
                throw new MongoSyncException(MongoSyncErrorCode.CONFIG_REQUIRED,
                        "sinkUri or sinkMongoClient is required");
            }
            if (blank(c.sourceDatabase) || blank(c.sourceCollection)) {
                throw new MongoSyncException(MongoSyncErrorCode.CONFIG_REQUIRED,
                        "source database/collection is required");
            }
            if (blank(c.sinkDatabase) || blank(c.sinkCollection)) {
                throw new MongoSyncException(MongoSyncErrorCode.CONFIG_REQUIRED,
                        "sink database/collection is required");
            }
            return c;
        }

        private static boolean blank(String s) {
            return s == null || s.trim().isEmpty();
        }
    }
}
