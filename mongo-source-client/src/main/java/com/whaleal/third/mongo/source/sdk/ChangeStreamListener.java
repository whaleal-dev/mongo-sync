package com.whaleal.third.mongo.source.sdk;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.whaleal.third.mongo.source.config.MongoSourceConfig;
import com.whaleal.third.mongo.source.converter.BsonToTransferEventConverter;
import com.whaleal.third.mongo.source.converter.ChangeStreamDdlConverter;
import com.whaleal.third.mongo.source.exception.SourceOffsetException;
import com.whaleal.third.mongo.source.model.ResumeToken;
import com.whaleal.third.mongo.source.spi.ResumeTokenStorage;
import com.whaleal.third.mongo.transfer.model.DdlEvent;
import com.whaleal.third.mongo.transfer.model.TransferEvent;
import com.whaleal.third.mongo.transfer.spi.DdlEventListener;
import com.whaleal.third.mongo.transfer.spi.TransferEventListener;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;

import java.util.concurrent.TimeUnit;

public class ChangeStreamListener extends AbstractSourceListener {

    /** 全量开始前的 clusterTime，增量用 startAtOperationTime 对齐窗口。 */
    private volatile BsonTimestamp startAtOperationTime;

    /** FULL_AND_CATCH_UP：全量结束后的 clusterTime 上界。 */
    private volatile BsonTimestamp catchUpEndTs;

    public ChangeStreamListener(MongoSourceConfig config) {
        super(config);
    }

    @Override
    protected String listenerThreadName() {
        return "mongo-source-listener";
    }

    @Override
    protected void beforeInitialSync() {
        ensureConnection();
        // 快照开始前记下 clusterTime，并清旧 token；随后与全量并行开增量
        startAtOperationTime = readClusterTime();
        saveResumeToken(ResumeToken.empty(), startAtOperationTime);
        reportOffsetProgress("changeStream-start", startAtOperationTime, "phase=beforeFull");
    }

    @Override
    protected void onInitialSyncCompleted() {
        // 并行增量可能已推进 ResumeToken，禁止清空；仅设置追平上界
        if (config.isCatchUpThenStop()) {
            catchUpEndTs = readClusterTime();
        }
    }

    @Override
    protected void startIncremental() {
        int retryCount = 0;
        ResumeToken resumeToken = loadResumeToken();

        while (running.get()) {
            try {
                ensureConnection();
                BsonTimestamp endTs = resolveEndTimestamp();
                MongoDatabase database = mongoClient.getDatabase(config.getDatabase());
                MongoCollection<BsonDocument> collection = database.getCollection(config.getCollection(), BsonDocument.class);

                ChangeStreamIterable<BsonDocument> changeStreamIterable = buildChangeStream(collection, resumeToken);
                // 并行追平或已有上界时用短 await，便于感知 catchUpEndTs
                if (endTs != null || config.isCatchUpThenStop()) {
                    changeStreamIterable = changeStreamIterable.maxAwaitTime(2L, TimeUnit.SECONDS);
                }
                BsonToTransferEventConverter converter =
                        new BsonToTransferEventConverter(config.getDatabase(), config.getCollection());
                ChangeStreamDdlConverter ddlConverter =
                        new ChangeStreamDdlConverter(config.getDatabase(), config.getCollection());
                TransferEventListener listener = config.getListener();
                DdlEventListener ddlListener = config.getDdlListener();

                try (MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor =
                             changeStreamIterable.cursor()) {
                    while (running.get()) {
                        ChangeStreamDocument<BsonDocument> changeStreamDocument;
                        if (endTs != null || config.isCatchUpThenStop()) {
                            changeStreamDocument = cursor.tryNext();
                            if (changeStreamDocument == null) {
                                endTs = resolveEndTimestamp();
                                if (endTs != null && shouldStopCatchUp(endTs)) {
                                    running.set(false);
                                    break;
                                }
                                if (endTs == null) {
                                    continue;
                                }
                                // 已有上界但仍无事件：继续等或判定追平完成
                                continue;
                            }
                        } else {
                            if (!cursor.hasNext()) {
                                break;
                            }
                            changeStreamDocument = cursor.next();
                        }

                        endTs = resolveEndTimestamp();
                        BsonTimestamp clusterTime = changeStreamDocument.getClusterTime();
                        if (endTs != null && clusterTime != null && clusterTime.compareTo(endTs) > 0) {
                            running.set(false);
                            break;
                        }

                        processChange(changeStreamDocument, converter, ddlConverter, listener, ddlListener);
                        ResumeToken latest = loadResumeToken();
                        if (!latest.isEmpty()) {
                            resumeToken = latest;
                        }
                        retryCount = 0;
                    }
                }
                if (!running.get()) {
                    break;
                }
            } catch (Exception e) {
                if (!running.get()) {
                    break;
                }
                retryCount = handleRetry(e, retryCount);
                resumeToken = loadResumeToken();
            }
        }
    }

    private void processChange(ChangeStreamDocument<BsonDocument> changeStreamDocument,
                               BsonToTransferEventConverter converter,
                               ChangeStreamDdlConverter ddlConverter,
                               TransferEventListener listener,
                               DdlEventListener ddlListener) {
        String operationType = changeStreamDocument.getOperationTypeString();
        BsonDocument eventDoc = toEventDocument(changeStreamDocument, operationType);
        BsonDocument token = changeStreamDocument.getResumeToken();

        if (ChangeStreamDdlConverter.isIgnorable(operationType)) {
            if (token != null) {
                saveResumeToken(ResumeToken.fromBson(token), changeStreamDocument.getClusterTime());
            }
            return;
        }

        if (ChangeStreamDdlConverter.isDdlOperation(operationType)) {
            DdlEvent ddlEvent = ddlConverter.convert(eventDoc);
            if (ddlEvent != null) {
                // drop/rename/dropDatabase：源端全量扫描视为完成
                maybeCompleteFullSyncOnNsChange(ddlEvent);
                if (ddlListener != null) {
                    ddlListener.onDdl(ddlEvent);
                }
            }
        } else if (isCrud(operationType)) {
            TransferEvent event = converter.convert(eventDoc);
            listener.onEvent(event);
        }

        if (token != null) {
            saveResumeToken(ResumeToken.fromBson(token), changeStreamDocument.getClusterTime());
        }
    }

    private boolean shouldStopCatchUp(BsonTimestamp endTs) {
        BsonTimestamp now = readClusterTime();
        return now != null && now.compareTo(endTs) >= 0;
    }

    private BsonTimestamp resolveEndTimestamp() {
        if (catchUpEndTs != null) {
            return catchUpEndTs;
        }
        return config.getOplogEndTimestamp();
    }

    private BsonTimestamp readClusterTime() {
        try {
            BsonDocument hello = mongoClient.getDatabase("admin")
                    .runCommand(new BsonDocument("hello", new BsonInt32(1)), BsonDocument.class);
            if (hello.containsKey("$clusterTime") && hello.get("$clusterTime").isDocument()) {
                BsonDocument ct = hello.getDocument("$clusterTime");
                if (ct.containsKey("clusterTime") && ct.get("clusterTime").isTimestamp()) {
                    return ct.getTimestamp("clusterTime");
                }
            }
        } catch (Exception ignored) {
        }
        return new BsonTimestamp((int) (System.currentTimeMillis() / 1000L), 1);
    }

    private static boolean isCrud(String operationType) {
        return "insert".equals(operationType)
                || "update".equals(operationType)
                || "replace".equals(operationType)
                || "delete".equals(operationType);
    }

    private BsonDocument toEventDocument(ChangeStreamDocument<BsonDocument> changeStreamDocument,
                                         String operationType) {
        BsonDocument eventDoc = new BsonDocument();
        if (operationType != null) {
            eventDoc.put("operationType", new BsonString(operationType));
        }

        BsonDocument fullDoc = changeStreamDocument.getFullDocument();
        if (fullDoc != null) {
            eventDoc.put("fullDocument", fullDoc);
        }

        BsonDocument preImage = changeStreamDocument.getFullDocumentBeforeChange();
        if (preImage != null) {
            eventDoc.put("fullDocumentBeforeChange", preImage);
        }

        BsonDocument documentKey = changeStreamDocument.getDocumentKey();
        if (documentKey != null) {
            eventDoc.put("documentKey", documentKey);
        }

        if (changeStreamDocument.getClusterTime() != null) {
            eventDoc.put("clusterTime", changeStreamDocument.getClusterTime());
        }

        BsonDocument token = changeStreamDocument.getResumeToken();
        if (token != null) {
            eventDoc.put("_id", token);
        }

        if (changeStreamDocument.getNamespace() != null) {
            BsonDocument ns = new BsonDocument();
            ns.put("db", new BsonString(changeStreamDocument.getNamespace().getDatabaseName()));
            if (changeStreamDocument.getNamespace().getCollectionName() != null) {
                ns.put("coll", new BsonString(changeStreamDocument.getNamespace().getCollectionName()));
            }
            eventDoc.put("ns", ns);
        } else if (changeStreamDocument.getNamespaceDocument() != null) {
            eventDoc.put("ns", changeStreamDocument.getNamespaceDocument());
        }

        if (changeStreamDocument.getDestinationNamespace() != null) {
            BsonDocument to = new BsonDocument();
            to.put("db", new BsonString(changeStreamDocument.getDestinationNamespace().getDatabaseName()));
            to.put("coll", new BsonString(changeStreamDocument.getDestinationNamespace().getCollectionName()));
            eventDoc.put("to", to);
        } else if (changeStreamDocument.getDestinationNamespaceDocument() != null) {
            eventDoc.put("to", changeStreamDocument.getDestinationNamespaceDocument());
        }

        if (changeStreamDocument.getUpdateDescription() != null) {
            BsonDocument updateDescription = new BsonDocument();
            if (changeStreamDocument.getUpdateDescription().getUpdatedFields() != null) {
                updateDescription.put("updatedFields", changeStreamDocument.getUpdateDescription().getUpdatedFields());
            }
            if (changeStreamDocument.getUpdateDescription().getRemovedFields() != null) {
                BsonArray removed = new BsonArray();
                for (String field : changeStreamDocument.getUpdateDescription().getRemovedFields()) {
                    removed.add(new BsonString(field));
                }
                updateDescription.put("removedFields", removed);
            }
            eventDoc.put("updateDescription", updateDescription);
        }

        BsonDocument extra = changeStreamDocument.getExtraElements();
        if (extra != null) {
            if (extra.containsKey("operationDescription") && extra.get("operationDescription").isDocument()) {
                eventDoc.put("operationDescription", extra.getDocument("operationDescription"));
            }
            if (!eventDoc.containsKey("ns") && extra.containsKey("ns") && extra.get("ns").isDocument()) {
                eventDoc.put("ns", extra.getDocument("ns"));
            }
        }
        return eventDoc;
    }

    private ChangeStreamIterable<BsonDocument> buildChangeStream(MongoCollection<BsonDocument> collection,
                                                                 ResumeToken resumeToken) {
        ChangeStreamIterable<BsonDocument> changeStreamIterable;

        if (config.getPipeline() != null && !config.getPipeline().isEmpty()) {
            changeStreamIterable = collection.watch(config.getPipeline(), BsonDocument.class);
        } else {
            changeStreamIterable = collection.watch(BsonDocument.class);
        }

        FullDocument fullDocumentMode = FullDocument.DEFAULT;
        if (config.getFullDocument() != null) {
            switch (config.getFullDocument()) {
                case UPDATE_LOOKUP:
                    fullDocumentMode = FullDocument.UPDATE_LOOKUP;
                    break;
                case WHEN_AVAILABLE:
                    fullDocumentMode = FullDocument.WHEN_AVAILABLE;
                    break;
                case REQUIRED:
                    fullDocumentMode = FullDocument.REQUIRED;
                    break;
                default:
                    fullDocumentMode = FullDocument.DEFAULT;
            }
        }
        changeStreamIterable = changeStreamIterable.fullDocument(fullDocumentMode);

        if (config.isEnablePreImage()) {
            changeStreamIterable = changeStreamIterable.fullDocumentBeforeChange(FullDocumentBeforeChange.WHEN_AVAILABLE);
        }

        if (resumeToken != null && !resumeToken.isEmpty()) {
            changeStreamIterable = changeStreamIterable.resumeAfter(resumeToken.getToken());
        } else {
            BsonTimestamp start = startAtOperationTime != null
                    ? startAtOperationTime
                    : config.getOplogStartTimestamp();
            if (start != null) {
                changeStreamIterable = changeStreamIterable.startAtOperationTime(start);
            }
        }

        return changeStreamIterable;
    }

    private ResumeToken loadResumeToken() {
        ResumeTokenStorage storage = config.getResumeTokenStorage();
        if (storage == null) {
            return ResumeToken.empty();
        }
        try {
            return storage.load();
        } catch (Exception e) {
            throw new SourceOffsetException("Failed to load resume token", e);
        }
    }

    private void saveResumeToken(ResumeToken token) {
        saveResumeToken(token, null);
    }

    private void saveResumeToken(ResumeToken token, BsonTimestamp clusterTime) {
        ResumeTokenStorage storage = config.getResumeTokenStorage();
        if (storage != null) {
            try {
                storage.save(token);
            } catch (Exception e) {
                logOffsetSnapshot("save-failed");
                throw new SourceOffsetException("Failed to save resume token", e);
            }
        }
        String detail = null;
        if (token != null && !token.isEmpty() && token.getToken() != null) {
            String json = token.getToken().toJson();
            if (json.length() > 120) {
                json = json.substring(0, 117) + "...";
            }
            detail = "resumeToken=" + json;
        }
        reportOffsetProgress("changeStream", clusterTime, detail);
    }
}
