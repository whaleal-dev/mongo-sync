package com.whaleal.third.mongo.sink.kafka.codec;

import com.whaleal.third.mongo.sink.converter.MapToBsonConverter;
import com.whaleal.third.mongo.transfer.model.DdlEvent;
import com.whaleal.third.mongo.transfer.model.DdlType;
import com.whaleal.third.mongo.transfer.model.TransferEvent;
import com.whaleal.third.mongo.transfer.model.TransferSource;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * 将 {@link TransferEvent} / {@link DdlEvent} 编码为 mongo-kafka Source 兼容的 Change Stream 文档。
 * <p>
 * 下游可用 {@code com.mongodb.kafka.connect.sink.cdc.mongodb.ChangeStreamHandler} 消费 CRUD。
 */
public final class ChangeStreamDocumentEncoder {

    private static final JsonWriterSettings JSON_SETTINGS =
            JsonWriterSettings.builder().outputMode(JsonMode.STRICT).build();

    private ChangeStreamDocumentEncoder() {
    }

    public static BsonDocument encode(TransferEvent event, String database, String collection) {
        if (event == null || event.getOp() == null) {
            return null;
        }
        String op = event.getOp().toLowerCase();
        String operationType;
        boolean operatorUpdate = false;
        switch (op) {
            case "c":
            case "r":
                operationType = "insert";
                break;
            case "u":
                operatorUpdate = isOperatorUpdate(event.getAfter());
                operationType = operatorUpdate ? "update" : "replace";
                break;
            case "d":
                operationType = "delete";
                break;
            default:
                return null;
        }

        String db = resolveDb(event, database);
        String coll = resolveColl(event, collection);

        BsonDocument doc = new BsonDocument();
        doc.put("operationType", new BsonString(operationType));
        doc.put("ns", ns(db, coll));

        BsonTimestamp clusterTime = clusterTimeOf(event);
        if (clusterTime != null) {
            doc.put("clusterTime", clusterTime);
        }

        BsonDocument documentKey = documentKey(event);
        if (documentKey != null && !documentKey.isEmpty()) {
            doc.put("documentKey", documentKey);
        }

        if ("insert".equals(operationType) || "replace".equals(operationType)) {
            BsonDocument full = MapToBsonConverter.toDocumentWithObjectId(event.getAfter(), "_id");
            if (full != null) {
                doc.put("fullDocument", full);
            }
        } else if ("update".equals(operationType) && operatorUpdate) {
            doc.put("updateDescription", toUpdateDescription(event.getAfter()));
            BsonDocument before = MapToBsonConverter.toDocumentWithObjectId(event.getBefore(), "_id");
            if (before != null && looksLikeFullDocument(before)) {
                doc.put("fullDocumentBeforeChange", before);
            }
        } else if ("delete".equals(operationType)) {
            BsonDocument before = MapToBsonConverter.toDocumentWithObjectId(event.getBefore(), "_id");
            if (before != null && looksLikeFullDocument(before)) {
                doc.put("fullDocumentBeforeChange", before);
            }
        }

        doc.put("_id", new BsonDocument("_data", new BsonString(syntheticResumeToken(event, documentKey))));
        return doc;
    }

    public static BsonDocument encodeDdl(DdlEvent event, String database, String collection) {
        if (event == null || event.getType() == null) {
            return null;
        }
        String db = event.getDatabase() != null ? event.getDatabase() : database;
        String coll = event.getCollection() != null ? event.getCollection() : collection;
        String operationType;
        switch (event.getType()) {
            case CREATE_COLLECTION:
                operationType = "create";
                break;
            case DROP_COLLECTION:
                operationType = "drop";
                break;
            case DROP_DATABASE:
                operationType = "dropDatabase";
                coll = null;
                break;
            case RENAME_COLLECTION:
                operationType = "rename";
                break;
            case CREATE_INDEXES:
                operationType = "createIndexes";
                break;
            case DROP_INDEXES:
                operationType = "dropIndexes";
                break;
            default:
                return null;
        }

        BsonDocument doc = new BsonDocument();
        doc.put("operationType", new BsonString(operationType));
        doc.put("ns", ns(db, coll));
        if (event.getTs() != null) {
            doc.put("clusterTime", event.getTs());
        } else if (event.getWallTimeMs() != null) {
            doc.put("clusterTime", millisToTimestamp(event.getWallTimeMs()));
        }
        if (event.getType() == DdlType.RENAME_COLLECTION) {
            BsonDocument to = extractRenameTo(event.getCommand(), db);
            if (to != null) {
                doc.put("to", to);
            }
        }
        if (event.getCommand() != null && !event.getCommand().isEmpty()) {
            doc.put("operationDescription", event.getCommand());
        }
        String token = operationType + ":" + db + "." + (coll == null ? "" : coll);
        if (event.getTs() != null) {
            token = event.getTs().getTime() + "-" + event.getTs().getInc() + "-" + token;
        }
        doc.put("_id", new BsonDocument("_data", new BsonString(token)));
        return doc;
    }

    public static String toJson(BsonDocument document) {
        return document.toJson(JSON_SETTINGS);
    }

    public static byte[] toJsonBytes(BsonDocument document) {
        return toJson(document).getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] toBsonBytes(BsonDocument document) {
        RawBsonDocument raw = new RawBsonDocument(document, new BsonDocumentCodec());
        ByteBuffer buf = raw.getByteBuffer().asNIO();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return bytes;
    }

    public static String keyJson(BsonDocument changeStreamDocument) {
        if (changeStreamDocument == null) {
            return "";
        }
        BsonValue key = changeStreamDocument.get("documentKey");
        if (key != null && key.isDocument()) {
            return key.asDocument().toJson(JSON_SETTINGS);
        }
        BsonValue id = changeStreamDocument.get("_id");
        if (id != null && id.isDocument()) {
            return id.asDocument().toJson(JSON_SETTINGS);
        }
        return "";
    }

    static boolean isOperatorUpdate(Map<String, Object> after) {
        if (after == null || after.isEmpty()) {
            return false;
        }
        boolean hasOperator = false;
        for (String key : after.keySet()) {
            if (key != null && key.startsWith("$")) {
                hasOperator = true;
            } else if (!"_id".equals(key)) {
                return false;
            }
        }
        return hasOperator;
    }

    @SuppressWarnings("unchecked")
    static BsonDocument toUpdateDescription(Map<String, Object> after) {
        BsonDocument desc = new BsonDocument();
        BsonDocument updated = new BsonDocument();
        BsonArray removed = new BsonArray();
        if (after != null) {
            Object set = after.get("$set");
            if (set instanceof Map) {
                BsonDocument setDoc = MapToBsonConverter.toDocument((Map<String, Object>) set);
                if (setDoc != null) {
                    updated = setDoc;
                }
            }
            Object unset = after.get("$unset");
            if (unset instanceof Map) {
                for (Object k : ((Map<?, ?>) unset).keySet()) {
                    if (k != null) {
                        removed.add(new BsonString(String.valueOf(k)));
                    }
                }
            }
        }
        desc.put("updatedFields", updated);
        desc.put("removedFields", removed);
        return desc;
    }

    private static BsonDocument documentKey(TransferEvent event) {
        Object id = idOf(event.getAfter());
        if (id == null) {
            id = idOf(event.getBefore());
        }
        if (id != null) {
            java.util.HashMap<String, Object> key = new java.util.HashMap<String, Object>(2);
            key.put("_id", id);
            return MapToBsonConverter.toDocumentWithObjectId(key, "_id");
        }
        if (event.getBefore() != null && !event.getBefore().isEmpty()
                && !isOperatorUpdate(event.getBefore())) {
            return MapToBsonConverter.toDocumentWithObjectId(event.getBefore(), "_id");
        }
        return new BsonDocument();
    }

    private static Object idOf(Map<String, Object> doc) {
        if (doc == null) {
            return null;
        }
        return doc.get("_id");
    }

    private static boolean looksLikeFullDocument(BsonDocument doc) {
        if (doc == null || doc.isEmpty()) {
            return false;
        }
        return doc.size() > 1 || !doc.containsKey("_id");
    }

    private static BsonDocument ns(String db, String coll) {
        BsonDocument ns = new BsonDocument();
        ns.put("db", new BsonString(db == null ? "" : db));
        if (coll != null && !coll.isEmpty()) {
            ns.put("coll", new BsonString(coll));
        }
        return ns;
    }

    private static String resolveDb(TransferEvent event, String fallback) {
        TransferSource src = event.getSource();
        if (src != null && src.getDb() != null && !src.getDb().isEmpty()) {
            return src.getDb();
        }
        return fallback;
    }

    private static String resolveColl(TransferEvent event, String fallback) {
        TransferSource src = event.getSource();
        if (src != null && src.getCollection() != null && !src.getCollection().isEmpty()) {
            return src.getCollection();
        }
        return fallback;
    }

    private static BsonTimestamp clusterTimeOf(TransferEvent event) {
        if (event.getSource() != null && event.getSource().getClusterTime() != null) {
            return millisToTimestamp(event.getSource().getClusterTime());
        }
        if (event.getTsMs() != null) {
            return millisToTimestamp(event.getTsMs());
        }
        return null;
    }

    private static BsonTimestamp millisToTimestamp(long ms) {
        int seconds = (int) (ms / 1000L);
        return new BsonTimestamp(seconds, 1);
    }

    private static String syntheticResumeToken(TransferEvent event, BsonDocument documentKey) {
        long ts = event.getTsMs() != null ? event.getTsMs() : 0L;
        String key = documentKey == null ? "" : documentKey.toJson(JSON_SETTINGS);
        return ts + "-" + event.getOp() + "-" + key;
    }

    private static BsonDocument extractRenameTo(BsonDocument command, String fallbackDb) {
        if (command == null || !command.containsKey("to")) {
            return null;
        }
        BsonValue to = command.get("to");
        if (to != null && to.isDocument()) {
            return to.asDocument();
        }
        if (to != null && to.isString()) {
            String v = to.asString().getValue();
            String db = fallbackDb;
            String coll = v;
            int dot = v.indexOf('.');
            if (dot > 0) {
                db = v.substring(0, dot);
                coll = v.substring(dot + 1);
            }
            return ns(db, coll);
        }
        return null;
    }
}
