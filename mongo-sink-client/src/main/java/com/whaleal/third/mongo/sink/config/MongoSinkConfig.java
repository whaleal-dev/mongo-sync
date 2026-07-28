package com.whaleal.third.mongo.sink.config;

import com.mongodb.client.MongoClient;

import java.util.concurrent.ExecutorService;

public class MongoSinkConfig {

    private String uri;
    private MongoClient mongoClient;
    private boolean closeMongoClientOnClose;
    private String database;
    private String collection;
    private WriteMode writeMode;
    private OnConflict onConflict;
    private String idField;
    private int batchSize;
    private boolean ordered;
    private boolean bypassDocumentValidation;
    private int writerThreads;
    private int writerQueueCapacity;
    private ExecutorService executor;
    private boolean shutdownExecutorOnClose;

    public static final int DEFAULT_BATCH_SIZE = 1000;
    public static final String DEFAULT_ID_FIELD = "_id";
    /** 默认写线程数；不同集合（ns）写入可并发，同 ns 内仍靠调用方/分桶保证有序。 */
    public static final int DEFAULT_WRITER_THREADS = 8;
    public static final int DEFAULT_WRITER_QUEUE_CAPACITY = 64;

    private MongoSinkConfig() {
    }

    public String getUri() {
        return uri;
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public boolean isCloseMongoClientOnClose() {
        return closeMongoClientOnClose;
    }

    public String getDatabase() {
        return database;
    }

    public String getCollection() {
        return collection;
    }

    public WriteMode getWriteMode() {
        return writeMode;
    }

    public OnConflict getOnConflict() {
        return onConflict;
    }

    public String getIdField() {
        return idField;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public boolean isOrdered() {
        return ordered;
    }

    public boolean isBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public int getWriterThreads() {
        return writerThreads;
    }

    public int getWriterQueueCapacity() {
        return writerQueueCapacity;
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public boolean isShutdownExecutorOnClose() {
        return shutdownExecutorOnClose;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String uri;
        private MongoClient mongoClient;
        private boolean closeMongoClientOnClose = true;
        private String database;
        private String collection;
        private WriteMode writeMode = WriteMode.UPSERT;
        private OnConflict onConflict = OnConflict.FAIL;
        private String idField = DEFAULT_ID_FIELD;
        private int batchSize = DEFAULT_BATCH_SIZE;
        private boolean ordered = false;
        private boolean bypassDocumentValidation = false;
        private int writerThreads = DEFAULT_WRITER_THREADS;
        private int writerQueueCapacity = DEFAULT_WRITER_QUEUE_CAPACITY;
        private ExecutorService executor;
        private boolean shutdownExecutorOnClose = true;

        public Builder uri(String uri) {
            this.uri = uri;
            return this;
        }

        /**
         * 复用已有 MongoClient（连接池常驻）。设置后不再按 uri 新建；
         * 默认 close 时不关闭该外部 client。
         */
        public Builder mongoClient(MongoClient mongoClient) {
            this.mongoClient = mongoClient;
            this.closeMongoClientOnClose = false;
            return this;
        }

        public Builder closeMongoClientOnClose(boolean closeMongoClientOnClose) {
            this.closeMongoClientOnClose = closeMongoClientOnClose;
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

        public Builder writeMode(WriteMode writeMode) {
            this.writeMode = writeMode == null ? WriteMode.UPSERT : writeMode;
            return this;
        }

        /**
         * 主键 / 唯一键冲突策略：{@link OnConflict#FAIL}（默认）、
         * {@link OnConflict#SKIP}（跳过+日志）、{@link OnConflict#UPSERT}（转为 upsert）。
         */
        public Builder onConflict(OnConflict onConflict) {
            this.onConflict = onConflict == null ? OnConflict.FAIL : onConflict;
            return this;
        }

        public Builder idField(String idField) {
            this.idField = idField == null || idField.trim().isEmpty() ? DEFAULT_ID_FIELD : idField;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
            return this;
        }

        public Builder ordered(boolean ordered) {
            this.ordered = ordered;
            return this;
        }

        public Builder bypassDocumentValidation(boolean bypassDocumentValidation) {
            this.bypassDocumentValidation = bypassDocumentValidation;
            return this;
        }

        public Builder writerThreads(int writerThreads) {
            this.writerThreads = writerThreads > 0 ? writerThreads : DEFAULT_WRITER_THREADS;
            return this;
        }

        public Builder writerQueueCapacity(int writerQueueCapacity) {
            this.writerQueueCapacity = writerQueueCapacity > 0
                    ? writerQueueCapacity
                    : DEFAULT_WRITER_QUEUE_CAPACITY;
            return this;
        }

        public Builder executor(ExecutorService executor) {
            this.executor = executor;
            this.shutdownExecutorOnClose = false;
            return this;
        }

        public Builder shutdownExecutorOnClose(boolean shutdownExecutorOnClose) {
            this.shutdownExecutorOnClose = shutdownExecutorOnClose;
            return this;
        }

        public MongoSinkConfig build() {
            if (mongoClient == null && (uri == null || uri.trim().isEmpty())) {
                throw new IllegalArgumentException("uri or mongoClient is required");
            }
            if (database == null || database.trim().isEmpty()) {
                throw new IllegalArgumentException("database is required");
            }
            if (collection == null || collection.trim().isEmpty()) {
                throw new IllegalArgumentException("collection is required");
            }

            MongoSinkConfig config = new MongoSinkConfig();
            config.uri = this.uri;
            config.mongoClient = this.mongoClient;
            config.closeMongoClientOnClose = this.mongoClient == null || this.closeMongoClientOnClose;
            config.database = this.database;
            config.collection = this.collection;
            config.writeMode = this.writeMode;
            config.onConflict = this.onConflict;
            config.idField = this.idField;
            config.batchSize = this.batchSize;
            config.ordered = this.ordered;
            config.bypassDocumentValidation = this.bypassDocumentValidation;
            config.writerThreads = this.writerThreads;
            config.writerQueueCapacity = this.writerQueueCapacity;
            config.executor = this.executor;
            config.shutdownExecutorOnClose = this.shutdownExecutorOnClose;
            return config;
        }
    }
}
