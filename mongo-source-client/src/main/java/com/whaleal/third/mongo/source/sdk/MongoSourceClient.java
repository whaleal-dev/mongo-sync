package com.whaleal.third.mongo.source.sdk;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.whaleal.third.mongo.source.config.CaptureMode;
import com.whaleal.third.mongo.source.config.MongoSourceConfig;
import com.whaleal.third.mongo.source.config.SyncMode;
import com.whaleal.third.mongo.source.oplog.MongoVersion;
import com.whaleal.third.mongo.source.topology.SourceTopologyDetector;
import com.whaleal.third.mongo.source.topology.SourceTopologyInfo;
import com.whaleal.third.mongo.transfer.spi.DdlEventListener;
import com.whaleal.third.mongo.transfer.spi.TransferEventListener;
import com.whaleal.third.mongo.source.spi.OplogOffsetStorage;
import com.whaleal.third.mongo.source.spi.ResumeTokenStorage;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import java.util.List;

public class MongoSourceClient {

    private final MongoSourceConfig config;
    private final SourceListener listener;

    private MongoSourceClient(MongoSourceConfig config) {
        this.config = config;
        this.listener = createListener(config);
    }

    private static SourceListener createListener(MongoSourceConfig config) {
        if (config.getCaptureMode() == CaptureMode.OPLOG) {
            return new OplogListener(config);
        }
        return new ChangeStreamListener(config);
    }

    public static Builder builder() {
        return new Builder();
    }

    public void start() {
        listener.start();
    }

    public void stop() {
        listener.stop();
    }

    public MongoSourceConfig getConfig() {
        return config;
    }

    public static class Builder {

        private String uri;
        private MongoClient mongoClient;
        private Boolean closeMongoClientOnStop;
        private String database;
        private String collection;
        private CaptureMode captureMode = CaptureMode.AUTO;
        private MongoVersion mongoVersion;
        private MongoSourceConfig.FullDocumentMode fullDocument = MongoSourceConfig.FullDocumentMode.DEFAULT;
        private boolean enablePreImage = false;
        private List<BsonDocument> pipeline;
        private int retryMaxTimes = MongoSourceConfig.DEFAULT_RETRY_MAX_TIMES;
        private long retryIntervalMs = MongoSourceConfig.DEFAULT_RETRY_INTERVAL_MS;
        private SyncMode syncMode = SyncMode.INCREMENTAL;
        private ResumeTokenStorage resumeTokenStorage;
        private OplogOffsetStorage oplogOffsetStorage;
        private TransferEventListener listener;
        private DdlEventListener ddlListener;
        private int listenerThreadPriority = MongoSourceConfig.DEFAULT_THREAD_PRIORITY;
        private BsonTimestamp oplogStartTimestamp;
        private BsonTimestamp oplogEndTimestamp;
        private int oplogBatchSize = MongoSourceConfig.DEFAULT_OPLOG_BATCH_SIZE;
        private boolean includeFromMigrate = false;
        private Runnable afterFullSyncBarrier;
        private int offsetLogIntervalSeconds = MongoSourceConfig.DEFAULT_OFFSET_LOG_INTERVAL_SECONDS;
        private int fullSyncParallelism = MongoSourceConfig.DEFAULT_FULL_SYNC_PARALLELISM;
        private int fullSyncBatchSize = MongoSourceConfig.DEFAULT_FULL_SYNC_BATCH_SIZE;
        private int fullSyncTaskMbSize = MongoSourceConfig.DEFAULT_FULL_SYNC_TASK_MB_SIZE;

        public Builder uri(String uri) {
            this.uri = uri;
            return this;
        }

        /**
         * 复用外部 MongoClient（推荐：进程内单例连接池，与 Sink 语义一致）。
         */
        public Builder mongoClient(MongoClient mongoClient) {
            this.mongoClient = mongoClient;
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
            this.captureMode = captureMode;
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

        public Builder fullDocument(MongoSourceConfig.FullDocumentMode fullDocument) {
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

        public Builder syncMode(SyncMode syncMode) {
            this.syncMode = syncMode == null ? SyncMode.INCREMENTAL : syncMode;
            return this;
        }

        /**
         * ChangeStream 位点：ResumeToken。
         */
        public Builder resumeTokenStorage(ResumeTokenStorage resumeTokenStorage) {
            this.resumeTokenStorage = resumeTokenStorage;
            return this;
        }

        /**
         * Oplog 位点：时间戳（{@link com.whaleal.third.mongo.source.model.OplogOffset}）。
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

        public Builder oplogEndTimestamp(BsonTimestamp oplogEndTimestamp) {
            this.oplogEndTimestamp = oplogEndTimestamp;
            return this;
        }

        public Builder oplogBatchSize(int oplogBatchSize) {
            this.oplogBatchSize = oplogBatchSize;
            return this;
        }

        public Builder includeFromMigrate(boolean includeFromMigrate) {
            this.includeFromMigrate = includeFromMigrate;
            return this;
        }

        public Builder afterFullSyncBarrier(Runnable afterFullSyncBarrier) {
            this.afterFullSyncBarrier = afterFullSyncBarrier;
            return this;
        }

        /**
         * 周期性打印位点到 stderr 的间隔（秒）。默认 30；{@code <=0} 关闭。
         */
        public Builder offsetLogIntervalSeconds(int offsetLogIntervalSeconds) {
            this.offsetLogIntervalSeconds = offsetLogIntervalSeconds;
            return this;
        }

        /** 全量并行读线程数；>1 时按 _id 切段并行（对齐 d2t）。 */
        public Builder fullSyncParallelism(int fullSyncParallelism) {
            this.fullSyncParallelism = fullSyncParallelism;
            return this;
        }

        public Builder fullSyncBatchSize(int fullSyncBatchSize) {
            this.fullSyncBatchSize = fullSyncBatchSize;
            return this;
        }

        /** 单段全量任务目标体积（MB），默认 32。 */
        public Builder fullSyncTaskMbSize(int fullSyncTaskMbSize) {
            this.fullSyncTaskMbSize = fullSyncTaskMbSize;
            return this;
        }

        public MongoSourceClient build() {
            MongoClient detectClient = this.mongoClient;
            boolean createdForDetect = false;
            if (detectClient == null) {
                if (this.uri == null || this.uri.trim().isEmpty()) {
                    throw new IllegalArgumentException("uri or mongoClient is required");
                }
                detectClient = MongoClients.create(this.uri);
                createdForDetect = true;
            }

            CaptureMode mode = this.captureMode == null ? CaptureMode.AUTO : this.captureMode;
            MongoVersion version = this.mongoVersion;
            try {
                SourceTopologyInfo info = SourceTopologyDetector.detectForSource(
                        detectClient, mode, this.uri, this.syncMode);
                mode = info.getResolvedCaptureMode();
                if (version == null) {
                    version = info.getVersion();
                }
            } catch (RuntimeException e) {
                if (createdForDetect) {
                    try {
                        detectClient.close();
                    } catch (Exception ignored) {
                    }
                }
                throw e;
            }

            if (mode == CaptureMode.OPLOG && version == null) {
                if (createdForDetect) {
                    try {
                        detectClient.close();
                    } catch (Exception ignored) {
                    }
                }
                throw new IllegalArgumentException(
                        "mongoVersion could not be detected for OPLOG; pass .mongoVersion(...) explicitly");
            }

            MongoSourceConfig.Builder configBuilder = MongoSourceConfig.builder()
                    .uri(this.uri)
                    .database(this.database)
                    .collection(this.collection)
                    .captureMode(mode)
                    .fullDocument(this.fullDocument)
                    .enablePreImage(this.enablePreImage)
                    .pipeline(this.pipeline)
                    .retryMaxTimes(this.retryMaxTimes)
                    .retryIntervalMs(this.retryIntervalMs)
                    .syncMode(this.syncMode)
                    .resumeTokenStorage(this.resumeTokenStorage)
                    .oplogOffsetStorage(this.oplogOffsetStorage)
                    .listener(this.listener)
                    .ddlListener(this.ddlListener)
                    .listenerThreadPriority(this.listenerThreadPriority)
                    .oplogStartTimestamp(this.oplogStartTimestamp)
                    .oplogEndTimestamp(this.oplogEndTimestamp)
                    .oplogBatchSize(this.oplogBatchSize)
                    .includeFromMigrate(this.includeFromMigrate)
                    .afterFullSyncBarrier(this.afterFullSyncBarrier)
                    .offsetLogIntervalSeconds(this.offsetLogIntervalSeconds)
                    .fullSyncParallelism(this.fullSyncParallelism)
                    .fullSyncBatchSize(this.fullSyncBatchSize)
                    .fullSyncTaskMbSize(this.fullSyncTaskMbSize);
            if (version != null) {
                configBuilder.mongoVersion(version);
            }
            // 探测用 client 复用为运行 client，避免双连接池
            configBuilder.mongoClient(detectClient);
            if (this.closeMongoClientOnStop != null) {
                configBuilder.closeMongoClientOnStop(this.closeMongoClientOnStop);
            } else if (createdForDetect) {
                configBuilder.closeMongoClientOnStop(true);
            } else {
                configBuilder.closeMongoClientOnStop(false);
            }
            return new MongoSourceClient(configBuilder.build());
        }
    }
}
