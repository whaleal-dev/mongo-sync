package com.whaleal.third.mongo.source.sdk;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.whaleal.third.mongo.source.config.MongoSourceConfig;
import com.whaleal.third.mongo.source.exception.SourceOffsetException;
import com.whaleal.third.mongo.source.model.OplogOffset;
import com.whaleal.third.mongo.source.oplog.OplogFetcher;
import com.whaleal.third.mongo.source.oplog.OplogParseResult;
import com.whaleal.third.mongo.source.oplog.parser.AbstractOplogParser;
import com.whaleal.third.mongo.source.oplog.parser.OplogParserFactory;
import com.whaleal.third.mongo.source.spi.OplogOffsetStorage;
import com.whaleal.third.mongo.transfer.spi.DdlEventListener;
import com.whaleal.third.mongo.transfer.spi.TransferEventListener;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

/**
 * 基于 local.oplog.rs 的获取 + 版本感知解析。
 * <p>
 * 位点为 oplog {@code ts}（{@link org.bson.BsonTimestamp}），与 ChangeStream ResumeToken 分离。
 */
public class OplogListener extends AbstractSourceListener {

    /**
     * 全量开始前记下的 oplog ts；增量从该 ts 之后续读，避免快照期间变更丢失。
     */
    private volatile BsonTimestamp initialSyncStartTs;

    /**
     * 全量结束后的 oplog ts；{@link com.whaleal.third.mongo.source.config.SyncMode#FULL_AND_CATCH_UP} 用其作窗口终点。
     */
    private volatile BsonTimestamp catchUpEndTs;

    public OplogListener(MongoSourceConfig config) {
        super(config);
    }

    @Override
    protected String listenerThreadName() {
        return "mongo-source-oplog-listener";
    }

    @Override
    protected void beforeInitialSync() {
        ensureConnection();
        BsonTimestamp latest = createFetcher().readLatestTimestamp();
        if (latest == null) {
            latest = new BsonTimestamp((int) (System.currentTimeMillis() / 1000L), 1);
        }
        initialSyncStartTs = latest;
        saveOplogOffset(OplogOffset.of(latest));
    }

    @Override
    protected void onInitialSyncCompleted() {
        // 并行模式下增量已在跑，禁止回拨位点；仅补追平上界
        if (config.isCatchUpThenStop()) {
            ensureConnection();
            BsonTimestamp end = createFetcher().readLatestTimestamp();
            if (end == null) {
                end = initialSyncStartTs;
            }
            catchUpEndTs = end;
        }
    }

    @Override
    protected void startIncremental() {
        int retryCount = 0;
        OplogOffset offset;
        if (initialSyncStartTs != null && !isInitialSyncFinished()) {
            offset = OplogOffset.of(initialSyncStartTs);
            saveOplogOffset(offset);
        } else {
            offset = resolveStartOffset();
        }

        while (running.get()) {
            try {
                ensureConnection();
                OplogFetcher fetcher = createFetcher();
                MongoCollection<BsonDocument> sourceCollection = mongoClient
                        .getDatabase(config.getDatabase())
                        .getCollection(config.getCollection(), BsonDocument.class);

                AbstractOplogParser parser = OplogParserFactory.create(
                        config.getDatabase(),
                        config.getCollection(),
                        config.getMongoVersion(),
                        config.getFullDocument(),
                        sourceCollection);

                TransferEventListener eventListener = config.getListener();
                DdlEventListener ddlListener = config.getDdlListener();

                BsonTimestamp startTs = offset.getTimestamp();
                BsonTimestamp endAtOpen = resolveEndTimestamp();
                try (MongoCursor<BsonDocument> cursor = fetcher.openCursor(startTs, endAtOpen)) {
                    while (running.get()) {
                        if (!cursor.hasNext()) {
                            // tailable maxAwait 到期或有界游标耗尽
                            break;
                        }
                        BsonDocument entry = cursor.next();
                        BsonTimestamp entryTs = entry.containsKey("ts") ? entry.getTimestamp("ts") : null;

                        BsonTimestamp endNow = resolveEndTimestamp();
                        if (endNow != null && entryTs != null && entryTs.compareTo(endNow) > 0) {
                            running.set(false);
                            break;
                        }
                        // 全量结束出现上界：结束当前无界游标，下轮按有界重开
                        if (endAtOpen == null && endNow != null) {
                            if (entryTs != null) {
                                offset = OplogOffset.of(entryTs);
                                saveOplogOffset(offset);
                            }
                            break;
                        }

                        if (fetcher.shouldSkip(entry)) {
                            if (entryTs != null) {
                                offset = OplogOffset.of(entryTs);
                                saveOplogOffset(offset);
                            }
                            continue;
                        }

                        OplogParseResult result = parser.parse(entry);
                        BsonTimestamp ts = result.getTs() != null ? result.getTs() : entryTs;

                        switch (result.getKind()) {
                            case CRUD:
                                eventListener.onEvent(result.getTransferEvent());
                                break;
                            case DDL:
                                // drop/rename/dropDatabase：源端全量扫描视为完成
                                maybeCompleteFullSyncOnNsChange(result.getDdlEvent());
                                if (ddlListener != null) {
                                    ddlListener.onDdl(result.getDdlEvent());
                                }
                                break;
                            case SKIP:
                            default:
                                break;
                        }

                        if (ts != null) {
                            offset = OplogOffset.of(ts);
                            saveOplogOffset(offset);
                        }
                        retryCount = 0;
                    }
                }

                BsonTimestamp endNow = resolveEndTimestamp();
                if (endNow != null) {
                    if (!running.get()) {
                        break;
                    }
                    if (offset.getTimestamp() != null && offset.getTimestamp().compareTo(endNow) >= 0) {
                        running.set(false);
                        break;
                    }
                    // 有界重开继续追平；若有界已耗尽且位点仍低于 end，再开一轮
                }
            } catch (MongoException e) {
                if (!running.get()) {
                    break;
                }
                retryCount = handleRetry(e, retryCount);
                offset = resolveStartOffset();
            } catch (Exception e) {
                if (!running.get()) {
                    break;
                }
                retryCount = handleRetry(e, retryCount);
                offset = resolveStartOffset();
            }
        }
    }

    private BsonTimestamp resolveEndTimestamp() {
        if (catchUpEndTs != null) {
            return catchUpEndTs;
        }
        return config.getOplogEndTimestamp();
    }

    private OplogFetcher createFetcher() {
        return new OplogFetcher(
                mongoClient,
                config.getDatabase(),
                config.getCollection(),
                config.getOplogFormatVersion(),
                config.getOplogBatchSize(),
                config.isIncludeFromMigrate());
    }

    private OplogOffset resolveStartOffset() {
        OplogOffset stored = loadOplogOffset();
        if (!stored.isEmpty() && stored.getTimestamp() != null) {
            return stored;
        }
        if (config.getOplogStartTimestamp() != null) {
            return OplogOffset.of(config.getOplogStartTimestamp());
        }

        ensureConnection();
        BsonTimestamp latest = createFetcher().readLatestTimestamp();
        if (latest == null) {
            latest = new BsonTimestamp((int) (System.currentTimeMillis() / 1000L), 1);
        }
        OplogOffset token = OplogOffset.of(latest);
        saveOplogOffset(token);
        return token;
    }

    private OplogOffset loadOplogOffset() {
        OplogOffsetStorage storage = config.getOplogOffsetStorage();
        if (storage == null) {
            return OplogOffset.empty();
        }
        try {
            return storage.load();
        } catch (Exception e) {
            throw new SourceOffsetException("Failed to load oplog offset", e);
        }
    }

    private void saveOplogOffset(OplogOffset offset) {
        OplogOffsetStorage storage = config.getOplogOffsetStorage();
        if (storage != null) {
            try {
                storage.save(offset);
            } catch (Exception e) {
                logOffsetSnapshot("save-failed");
                throw new SourceOffsetException("Failed to save oplog offset", e);
            }
        }
        BsonTimestamp ts = offset == null ? null : offset.getTimestamp();
        reportOffsetProgress("oplog", ts, null);
    }
}
