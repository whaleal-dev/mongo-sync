package com.whaleal.third.mongo.sink.writer;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.whaleal.third.mongo.sink.config.MongoSinkConfig;
import com.whaleal.third.mongo.sink.config.OnConflict;
import com.whaleal.third.mongo.sink.config.WriteMode;
import com.whaleal.third.mongo.sink.converter.MapToBsonConverter;
import com.whaleal.third.mongo.sink.exception.SinkConnectException;
import com.whaleal.third.mongo.sink.exception.SinkWriteException;
import com.whaleal.third.mongo.sink.model.SinkWriteResult;
import com.whaleal.third.mongo.transfer.model.DdlEvent;
import com.whaleal.third.mongo.transfer.model.DdlType;
import com.whaleal.third.mongo.transfer.model.TransferEvent;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 缓冲 + 可选线程池并发 bulkWrite。
 */
public class SinkWriter implements AutoCloseable {

    private final MongoSinkConfig config;
    private final MongoClient mongoClient;
    private final boolean ownsMongoClient;
    /** 可在 rename 后切换到新集合名。 */
    private final AtomicReference<MongoCollection<BsonDocument>> collectionRef;
    private final AtomicReference<String> activeCollectionName;
    private final AtomicBoolean ordered;
    private final DdlApplier ddlApplier;
    private final List<WriteModel<BsonDocument>> buffer = new ArrayList<WriteModel<BsonDocument>>();
    private final Object lock = new Object();

    private final ExecutorService executor;
    private final boolean ownsExecutor;
    private final boolean async;
    private final List<Future<SinkWriteResult>> inflight = new ArrayList<Future<SinkWriteResult>>();
    private final AtomicReference<Throwable> asyncError = new AtomicReference<Throwable>();

    public SinkWriter(MongoSinkConfig config) {
        this.config = config;
        try {
            if (config.getMongoClient() != null) {
                this.mongoClient = config.getMongoClient();
                this.ownsMongoClient = config.isCloseMongoClientOnClose();
            } else {
                this.mongoClient = MongoClients.create(config.getUri());
                this.ownsMongoClient = true;
            }
            this.activeCollectionName = new AtomicReference<String>(config.getCollection());
            this.collectionRef = new AtomicReference<MongoCollection<BsonDocument>>(
                    mongoClient.getDatabase(config.getDatabase())
                            .getCollection(config.getCollection(), BsonDocument.class));
            this.ordered = new AtomicBoolean(config.isOrdered());
            this.ddlApplier = new DdlApplier(mongoClient, config);
        } catch (Exception e) {
            throw new SinkConnectException("Failed to connect sink MongoDB", e);
        }

        if (config.getExecutor() != null) {
            this.executor = config.getExecutor();
            this.ownsExecutor = config.isShutdownExecutorOnClose();
            this.async = true;
        } else if (config.getWriterThreads() > 1) {
            this.executor = createWriterPool(config.getWriterThreads(), config.getWriterQueueCapacity());
            this.ownsExecutor = true;
            this.async = true;
        } else {
            this.executor = null;
            this.ownsExecutor = false;
            this.async = false;
        }
    }

    /**
     * 唯一文档事件入口：只识别 {@link TransferEvent}（与捕获协议无关）。
     */
    public void apply(TransferEvent event) {
        if (event == null || event.getOp() == null) {
            return;
        }
        WriteModel<BsonDocument> model = toWriteModel(event);
        if (model != null) {
            enqueue(model);
        }
    }

    public void apply(String op, Map<String, Object> after, Map<String, Object> before) {
        apply(TransferEvent.builder().op(op).after(after).before(before).build());
    }

    /**
     * 执行 DDL：先 flush+等待在途 CRUD，再同步执行；rename 后切换写目标集合。
     */
    public void applyDdl(DdlEvent event) {
        flushAndWait();
        ddlApplier.apply(event);
        retargetAfterDdl(event);
    }

    /** 运行中按唯一索引策略调整 ordered bulk。 */
    public void setOrdered(boolean orderedWrite) {
        this.ordered.set(orderedWrite);
    }

    public boolean isOrdered() {
        return ordered.get();
    }

    public String getActiveCollection() {
        return activeCollectionName.get();
    }

    private void retargetAfterDdl(DdlEvent event) {
        if (event == null || event.getType() != DdlType.RENAME_COLLECTION) {
            return;
        }
        String toColl = extractRenameToCollection(event);
        if (toColl == null || toColl.isEmpty()) {
            return;
        }
        activeCollectionName.set(toColl);
        collectionRef.set(mongoClient.getDatabase(config.getDatabase())
                .getCollection(toColl, BsonDocument.class));
        System.err.println("[mongo-sink] retarget collection after rename -> "
                + config.getDatabase() + "." + toColl);
    }

    private static String extractRenameToCollection(DdlEvent event) {
        BsonDocument cmd = event.getCommand();
        if (cmd == null || !cmd.containsKey("to") || !cmd.get("to").isString()) {
            return null;
        }
        String to = cmd.getString("to").getValue();
        if (to.contains(".")) {
            return to.substring(to.indexOf('.') + 1);
        }
        return to;
    }

    public void insert(Map<String, Object> document) {
        apply(TransferEvent.builder().op("c").after(document).build());
    }

    public void replace(Map<String, Object> document) {
        apply(TransferEvent.builder().op("u").after(document).build());
    }

    public void update(Map<String, Object> filterDoc, Map<String, Object> updateDoc) {
        apply(TransferEvent.builder().op("u").before(filterDoc).after(updateDoc).build());
    }

    public void delete(Map<String, Object> documentKey) {
        apply(TransferEvent.builder().op("d").before(documentKey).build());
    }

    /**
     * 刷写本地缓冲：将剩余数据提交到写入线程池（或同步写出）。
     * <p>
     * <b>不会</b>关闭连接或线程池，也<b>不会</b>等待在途任务结束。
     * 需要等待时请调用 {@link #awaitPending()}；进程退出前调用 {@link #close()}。
     */
    public SinkWriteResult flush() {
        checkAsyncError();
        List<WriteModel<BsonDocument>> batch;
        synchronized (lock) {
            if (buffer.isEmpty()) {
                return SinkWriteResult.empty();
            }
            batch = new ArrayList<WriteModel<BsonDocument>>(buffer);
            buffer.clear();
        }
        if (!async || executor == null) {
            return doBulkWrite(batch);
        }
        submitOrWrite(batch);
        return SinkWriteResult.empty();
    }

    /**
     * 等待所有在途并发写入完成（不关闭资源）。
     */
    public SinkWriteResult awaitPending() {
        checkAsyncError();
        return awaitInflight();
    }

    /**
     * 刷写缓冲并等待在途任务完成（仍不关闭连接/线程池）。
     */
    public SinkWriteResult flushAndWait() {
        flush();
        return awaitPending();
    }

    public int bufferedSize() {
        synchronized (lock) {
            return buffer.size();
        }
    }

    public int inflightTaskCount() {
        synchronized (inflight) {
            compactInflightLocked();
            return inflight.size();
        }
    }

    @Override
    public void close() {
        try {
            // 仅在最终关闭时：刷剩余 + 等在途 + 关线程池 + 关连接
            flushAndWait();
        } finally {
            if (ownsExecutor && executor != null) {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
            // 仅关闭自建的 MongoClient；外部注入的连接由调用方管理
            if (ownsMongoClient && mongoClient != null) {
                mongoClient.close();
            }
        }
    }

    private void enqueue(WriteModel<BsonDocument> model) {
        checkAsyncError();
        List<WriteModel<BsonDocument>> batchToFlush = null;
        synchronized (lock) {
            buffer.add(model);
            if (buffer.size() >= config.getBatchSize()) {
                batchToFlush = new ArrayList<WriteModel<BsonDocument>>(buffer);
                buffer.clear();
            }
        }
        if (batchToFlush != null) {
            submitOrWrite(batchToFlush);
        }
    }

    private void submitOrWrite(List<WriteModel<BsonDocument>> batch) {
        if (!async || executor == null) {
            doBulkWrite(batch);
            return;
        }
        Future<SinkWriteResult> future = executor.submit(() -> doBulkWrite(batch));
        synchronized (inflight) {
            inflight.add(future);
            compactInflightLocked();
        }
    }

    private SinkWriteResult awaitInflight() {
        if (!async) {
            return SinkWriteResult.empty();
        }
        SinkWriteResult merged = SinkWriteResult.empty();
        // 循环直到列表空：等待期间新提交的 Future 也会被等到，避免 flushAndWait 漏等
        while (true) {
            Future<SinkWriteResult> next;
            synchronized (inflight) {
                if (inflight.isEmpty()) {
                    break;
                }
                next = inflight.remove(0);
            }
            try {
                SinkWriteResult part = next.get();
                merged = merged.merge(part);
            } catch (Exception e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                asyncError.compareAndSet(null, cause);
                throw new SinkWriteException("async bulkWrite failed", cause);
            }
        }
        checkAsyncError();
        return merged;
    }

    private void compactInflightLocked() {
        Iterator<Future<SinkWriteResult>> it = inflight.iterator();
        while (it.hasNext()) {
            Future<SinkWriteResult> f = it.next();
            if (f.isDone()) {
                it.remove();
                try {
                    f.get();
                } catch (Exception e) {
                    Throwable cause = e.getCause() != null ? e.getCause() : e;
                    asyncError.compareAndSet(null, cause);
                }
            }
        }
    }

    private void checkAsyncError() {
        Throwable err = asyncError.get();
        if (err != null) {
            throw new SinkWriteException("async bulkWrite previously failed", err);
        }
    }

    private SinkWriteResult doBulkWrite(List<WriteModel<BsonDocument>> batch) {
        if (batch == null || batch.isEmpty()) {
            return SinkWriteResult.empty();
        }
        SinkWriteResult merged = SinkWriteResult.empty();
        List<WriteModel<BsonDocument>> remaining = batch;
        int guard = 0;
        while (remaining != null && !remaining.isEmpty()) {
            if (++guard > batch.size() + 8) {
                throw new SinkWriteException("bulkWrite conflict handling exhausted, size=" + batch.size());
            }
            try {
                merged = merged.merge(executeBulkWrite(remaining));
                return merged;
            } catch (MongoBulkWriteException e) {
                ConflictContinuation cont = handleBulkWriteConflict(remaining, e);
                if (e.getWriteResult() != null) {
                    merged = merged.merge(SinkWriteResult.from(e.getWriteResult()));
                }
                if (cont == null) {
                    throw new SinkWriteException("bulkWrite failed, size=" + remaining.size(), e);
                }
                remaining = cont.remaining;
            } catch (MongoException e) {
                if (isDuplicateKeyException(e) && config.getOnConflict() != OnConflict.FAIL) {
                    logConflict("skip entire batch after duplicate key: " + e.getMessage());
                    return merged;
                }
                throw new SinkWriteException("bulkWrite failed, size=" + remaining.size(), e);
            }
        }
        return merged;
    }

    private SinkWriteResult executeBulkWrite(List<WriteModel<BsonDocument>> batch) {
        BulkWriteOptions options = new BulkWriteOptions()
                .ordered(ordered.get())
                .bypassDocumentValidation(config.isBypassDocumentValidation());
        BulkWriteResult result = collectionRef.get().bulkWrite(batch, options);
        return SinkWriteResult.from(result);
    }

    /**
     * @return 继续写入的剩余 models；null 表示应按失败抛出
     */
    private ConflictContinuation handleBulkWriteConflict(List<WriteModel<BsonDocument>> batch,
                                                         MongoBulkWriteException e) {
        List<BulkWriteError> errors = e.getWriteErrors();
        if (errors == null || errors.isEmpty()) {
            return null;
        }

        OnConflict policy = config.getOnConflict() == null ? OnConflict.FAIL : config.getOnConflict();
        if (policy == OnConflict.FAIL) {
            return null;
        }

        boolean allDuplicate = true;
        for (BulkWriteError err : errors) {
            if (!isDuplicateKeyCode(err.getCode())) {
                allDuplicate = false;
                break;
            }
        }
        if (!allDuplicate) {
            return null;
        }

        if (!ordered.get()) {
            if (policy == OnConflict.SKIP) {
                for (BulkWriteError err : errors) {
                    logConflict("SKIP unordered index=" + err.getIndex() + " msg=" + err.getMessage());
                }
                return ConflictContinuation.done();
            }
            // UPSERT：仅重试可转换为 upsert 的 InsertOne
            List<WriteModel<BsonDocument>> retry = new ArrayList<WriteModel<BsonDocument>>();
            for (BulkWriteError err : errors) {
                int i = err.getIndex();
                if (i < 0 || i >= batch.size()) {
                    continue;
                }
                WriteModel<BsonDocument> converted = convertInsertToUpsert(batch.get(i));
                if (converted != null) {
                    logConflict("UPSERT convert unordered index=" + i + " msg=" + err.getMessage());
                    retry.add(converted);
                } else {
                    logConflict("UPSERT fallback SKIP unordered index=" + i + " msg=" + err.getMessage());
                }
            }
            return retry.isEmpty() ? ConflictContinuation.done() : ConflictContinuation.of(retry);
        }

        // ordered：服务端在首个错误处停止，通常只有一个 writeError
        BulkWriteError first = errors.get(0);
        int idx = first.getIndex();
        if (idx < 0 || idx >= batch.size()) {
            return null;
        }

        if (policy == OnConflict.SKIP) {
            logConflict("SKIP ordered index=" + idx + " msg=" + first.getMessage());
            if (idx + 1 >= batch.size()) {
                return ConflictContinuation.done();
            }
            return ConflictContinuation.of(new ArrayList<WriteModel<BsonDocument>>(
                    batch.subList(idx + 1, batch.size())));
        }

        // UPSERT：尽量把冲突的 InsertOne 转为 ReplaceOne upsert，并从该 index 重试
        WriteModel<BsonDocument> failed = batch.get(idx);
        WriteModel<BsonDocument> converted = convertInsertToUpsert(failed);
        if (converted != null) {
            logConflict("UPSERT convert insert at index=" + idx + " msg=" + first.getMessage());
            List<WriteModel<BsonDocument>> retry = new ArrayList<WriteModel<BsonDocument>>(batch.size() - idx);
            retry.add(converted);
            if (idx + 1 < batch.size()) {
                retry.addAll(batch.subList(idx + 1, batch.size()));
            }
            return ConflictContinuation.of(retry);
        }

        logConflict("UPSERT fallback SKIP index=" + idx + " (not InsertOne or no _id) msg=" + first.getMessage());
        if (idx + 1 >= batch.size()) {
            return ConflictContinuation.done();
        }
        return ConflictContinuation.of(new ArrayList<WriteModel<BsonDocument>>(
                batch.subList(idx + 1, batch.size())));
    }

    private WriteModel<BsonDocument> convertInsertToUpsert(WriteModel<BsonDocument> model) {
        if (!(model instanceof InsertOneModel)) {
            return null;
        }
        @SuppressWarnings("unchecked")
        InsertOneModel<BsonDocument> insert = (InsertOneModel<BsonDocument>) model;
        BsonDocument doc = insert.getDocument();
        Bson filter = buildIdFilter(doc);
        if (filter == null) {
            return null;
        }
        return new ReplaceOneModel<BsonDocument>(filter, doc, new ReplaceOptions().upsert(true));
    }

    private static boolean isDuplicateKeyCode(int code) {
        return code == 11000 || code == 11001;
    }

    private static boolean isDuplicateKeyException(MongoException e) {
        if (isDuplicateKeyCode(e.getCode())) {
            return true;
        }
        String msg = e.getMessage();
        return msg != null && msg.contains("E11000");
    }

    private static void logConflict(String message) {
        System.err.println("[mongo-sink] duplicate-key " + message);
    }

    private static final class ConflictContinuation {
        private final List<WriteModel<BsonDocument>> remaining;

        private ConflictContinuation(List<WriteModel<BsonDocument>> remaining) {
            this.remaining = remaining;
        }

        static ConflictContinuation done() {
            return new ConflictContinuation(new ArrayList<WriteModel<BsonDocument>>());
        }

        static ConflictContinuation of(List<WriteModel<BsonDocument>> remaining) {
            return new ConflictContinuation(remaining);
        }
    }

    private static ExecutorService createWriterPool(int threads, int queueCapacity) {
        final AtomicInteger seq = new AtomicInteger(1);
        ThreadFactory factory = r -> {
            Thread t = new Thread(r, "mongo-sink-writer-" + seq.getAndIncrement());
            t.setDaemon(false);
            return t;
        };
        return new ThreadPoolExecutor(
                threads,
                threads,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(queueCapacity),
                factory,
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    private WriteModel<BsonDocument> toWriteModel(TransferEvent event) {
        String op = event.getOp().toLowerCase();
        switch (op) {
            case "c":
            case "r":
                return buildInsertOrUpsert(event.getAfter());
            case "u":
                return buildReplace(event.getAfter(), event.getBefore());
            case "d":
                return buildDelete(event.getBefore() != null ? event.getBefore() : event.getAfter());
            default:
                return null;
        }
    }

    private WriteModel<BsonDocument> buildInsertOrUpsert(Map<String, Object> after) {
        BsonDocument doc = MapToBsonConverter.toDocumentWithObjectId(after, config.getIdField());
        if (doc == null) {
            return null;
        }
        if (shouldUpsertInsert()) {
            Bson filter = buildIdFilter(doc);
            if (filter != null) {
                return new ReplaceOneModel<BsonDocument>(filter, doc, new ReplaceOptions().upsert(true));
            }
        }
        return new InsertOneModel<BsonDocument>(doc);
    }

    private boolean shouldUpsertInsert() {
        return shouldUpsertWrite() || config.getOnConflict() == OnConflict.UPSERT;
    }

    private boolean shouldUpsertWrite() {
        return config.getWriteMode() == WriteMode.UPSERT;
    }

    private WriteModel<BsonDocument> buildReplace(Map<String, Object> after, Map<String, Object> before) {
        BsonDocument doc = MapToBsonConverter.toDocumentWithObjectId(after, config.getIdField());
        if (doc == null) {
            return null;
        }

        // 优先用 before（documentKey / preImage）定位，与 Source Envelope 约定一致
        Bson filter = null;
        if (before != null) {
            BsonDocument beforeDoc = MapToBsonConverter.toDocumentWithObjectId(before, config.getIdField());
            filter = buildIdFilter(beforeDoc);
        }
        if (filter == null) {
            filter = buildIdFilter(doc);
        }
        if (filter == null) {
            throw new SinkWriteException("update/replace requires id field: " + config.getIdField());
        }

        if (isUpdateOperatorDocument(doc)) {
            BsonDocument updateDoc = stripIdFromUpdateOperators(doc);
            if (updateDoc.isEmpty()) {
                return null;
            }
            boolean upsert = shouldUpsertWrite();
            return new UpdateOneModel<BsonDocument>(filter, updateDoc, new UpdateOptions().upsert(upsert));
        }

        boolean upsert = shouldUpsertWrite();
        return new ReplaceOneModel<BsonDocument>(filter, doc, new ReplaceOptions().upsert(upsert));
    }

    private boolean isUpdateOperatorDocument(BsonDocument doc) {
        if (doc == null || doc.isEmpty()) {
            return false;
        }
        boolean hasOperator = false;
        for (String key : doc.keySet()) {
            if (key.startsWith("$")) {
                hasOperator = true;
            } else if (!config.getIdField().equals(key)) {
                return false;
            }
        }
        return hasOperator;
    }

    private BsonDocument stripIdFromUpdateOperators(BsonDocument doc) {
        BsonDocument update = new BsonDocument();
        for (Map.Entry<String, BsonValue> e : doc.entrySet()) {
            if (config.getIdField().equals(e.getKey())) {
                continue;
            }
            update.put(e.getKey(), e.getValue());
        }
        return update;
    }

    private WriteModel<BsonDocument> buildDelete(Map<String, Object> keyDoc) {
        BsonDocument key = MapToBsonConverter.toDocumentWithObjectId(keyDoc, config.getIdField());
        if (key == null || key.isEmpty()) {
            return null;
        }
        Bson filter = buildIdFilter(key);
        if (filter == null) {
            filter = key;
        }
        return new DeleteOneModel<BsonDocument>(filter);
    }

    private Bson buildIdFilter(BsonDocument doc) {
        if (doc == null || !doc.containsKey(config.getIdField())) {
            return null;
        }
        BsonValue id = doc.get(config.getIdField());
        return Filters.eq(config.getIdField(), id);
    }
}
