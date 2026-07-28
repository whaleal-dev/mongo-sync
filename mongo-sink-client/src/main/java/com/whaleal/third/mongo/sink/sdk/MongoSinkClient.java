package com.whaleal.third.mongo.sink.sdk;

import com.mongodb.client.MongoClient;
import com.whaleal.third.mongo.sink.config.MongoSinkConfig;
import com.whaleal.third.mongo.sink.config.OnConflict;
import com.whaleal.third.mongo.sink.config.WriteMode;
import com.whaleal.third.mongo.sink.model.SinkWriteResult;
import com.whaleal.third.mongo.sink.writer.SinkWriter;
import com.whaleal.third.mongo.transfer.model.DdlEvent;
import com.whaleal.third.mongo.transfer.model.TransferEvent;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * MongoDB Sink：只识别通用传输模型 {@link TransferEvent} / {@link DdlEvent}，
 * 不关心上游是 Oplog 还是 ChangeStream。
 */
public class MongoSinkClient implements AutoCloseable {

    private final MongoSinkConfig config;
    private final SinkWriter writer;

    private MongoSinkClient(MongoSinkConfig config) {
        this.config = config;
        this.writer = new SinkWriter(config);
    }

    public static Builder builder() {
        return new Builder();
    }

    public void write(TransferEvent event) {
        writer.apply(event);
    }

    public void write(String op, Map<String, Object> after, Map<String, Object> before) {
        writer.apply(op, after, before);
    }

    public void writeBatch(List<TransferEvent> events) {
        if (events == null) {
            return;
        }
        for (TransferEvent event : events) {
            writer.apply(event);
        }
    }

    public void insert(Map<String, Object> document) {
        writer.insert(document);
    }

    public void replace(Map<String, Object> document) {
        writer.replace(document);
    }

    public void update(Map<String, Object> filterDoc, Map<String, Object> updateDoc) {
        writer.update(filterDoc, updateDoc);
    }

    public void delete(Map<String, Object> documentKey) {
        writer.delete(documentKey);
    }

    public void applyDdl(DdlEvent event) {
        writer.applyDdl(event);
    }

    /** 运行中调整 ordered bulk（唯一索引 DDL 后由 Sync 调用）。 */
    public void setOrdered(boolean ordered) {
        writer.setOrdered(ordered);
    }

    public boolean isOrdered() {
        return writer.isOrdered();
    }

    public String getActiveCollection() {
        return writer.getActiveCollection();
    }

    /**
     * 刷写缓冲并提交到线程池，不等待在途任务，不关闭连接/线程池。
     */
    public SinkWriteResult flush() {
        return writer.flush();
    }

    /**
     * 等待在途并发写入完成（不关闭资源）。
     */
    public SinkWriteResult awaitPending() {
        return writer.awaitPending();
    }

    /**
     * 刷写缓冲并等待在途完成（不关闭资源）。
     */
    public SinkWriteResult flushAndWait() {
        return writer.flushAndWait();
    }

    public int bufferedSize() {
        return writer.bufferedSize();
    }

    public int inflightTaskCount() {
        return writer.inflightTaskCount();
    }

    public MongoSinkConfig getConfig() {
        return config;
    }

    @Override
    public void close() {
        writer.close();
    }

    public static class Builder {
        private String uri;
        private MongoClient mongoClient;
        private Boolean closeMongoClientOnClose;
        private String database;
        private String collection;
        private WriteMode writeMode = WriteMode.UPSERT;
        private OnConflict onConflict = OnConflict.FAIL;
        private String idField = MongoSinkConfig.DEFAULT_ID_FIELD;
        private int batchSize = MongoSinkConfig.DEFAULT_BATCH_SIZE;
        private boolean ordered = false;
        private boolean bypassDocumentValidation = false;
        private int writerThreads = MongoSinkConfig.DEFAULT_WRITER_THREADS;
        private int writerQueueCapacity = MongoSinkConfig.DEFAULT_WRITER_QUEUE_CAPACITY;
        private ExecutorService executor;
        private Boolean shutdownExecutorOnClose;

        public Builder uri(String uri) {
            this.uri = uri;
            return this;
        }

        /**
         * 复用外部 MongoClient（推荐：进程内单例连接池，避免反复创建销毁）。
         */
        public Builder mongoClient(MongoClient mongoClient) {
            this.mongoClient = mongoClient;
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
            this.writeMode = writeMode;
            return this;
        }

        /**
         * 主键/唯一键冲突：FAIL（默认）/ SKIP（跳过+日志）/ UPSERT（转为 upsert）。
         */
        public Builder onConflict(OnConflict onConflict) {
            this.onConflict = onConflict == null ? OnConflict.FAIL : onConflict;
            return this;
        }

        public Builder idField(String idField) {
            this.idField = idField;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
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
            this.writerThreads = writerThreads;
            return this;
        }

        public Builder writerQueueCapacity(int writerQueueCapacity) {
            this.writerQueueCapacity = writerQueueCapacity;
            return this;
        }

        public Builder executor(ExecutorService executor) {
            this.executor = executor;
            return this;
        }

        public Builder shutdownExecutorOnClose(boolean shutdownExecutorOnClose) {
            this.shutdownExecutorOnClose = shutdownExecutorOnClose;
            return this;
        }

        public MongoSinkClient build() {
            MongoSinkConfig.Builder configBuilder = MongoSinkConfig.builder()
                    .uri(this.uri)
                    .database(this.database)
                    .collection(this.collection)
                    .writeMode(this.writeMode)
                    .onConflict(this.onConflict)
                    .idField(this.idField)
                    .batchSize(this.batchSize)
                    .ordered(this.ordered)
                    .bypassDocumentValidation(this.bypassDocumentValidation)
                    .writerThreads(this.writerThreads)
                    .writerQueueCapacity(this.writerQueueCapacity);
            if (this.mongoClient != null) {
                configBuilder.mongoClient(this.mongoClient);
            }
            if (this.closeMongoClientOnClose != null) {
                configBuilder.closeMongoClientOnClose(this.closeMongoClientOnClose);
            }
            if (this.executor != null) {
                configBuilder.executor(this.executor);
            }
            if (this.shutdownExecutorOnClose != null) {
                configBuilder.shutdownExecutorOnClose(this.shutdownExecutorOnClose);
            }
            return new MongoSinkClient(configBuilder.build());
        }
    }
}
