package com.whaleal.third.mongo.source.converter;

import com.whaleal.third.mongo.source.exception.TransferEventConvertException;
import com.whaleal.third.mongo.transfer.model.TransferEvent;
import com.whaleal.third.mongo.transfer.model.TransferSource;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.BsonValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ChangeStream → TransferEvent。字段约定与 Sink 对齐：
 * <ul>
 *   <li>c/r — after = 全量文档</li>
 *   <li>u — after = 全量文档，或 {@code $set/$unset}；before = preImage 或 documentKey</li>
 *   <li>d — before = preImage 或 documentKey</li>
 * </ul>
 */
public class BsonToTransferEventConverter {

    private final String database;
    private final String collection;

    public BsonToTransferEventConverter(String database, String collection) {
        this.database = database;
        this.collection = collection;
    }

    public TransferEvent convert(BsonDocument changeStreamDocument) {
        try {
            String operationType = changeStreamDocument.getString("operationType").getValue();
            BsonDocument fullDocument = changeStreamDocument.getDocument("fullDocument", null);
            BsonDocument preImage = changeStreamDocument.getDocument("fullDocumentBeforeChange", null);
            BsonDocument documentKey = changeStreamDocument.getDocument("documentKey", null);
            BsonTimestamp clusterTime = changeStreamDocument.getTimestamp("clusterTime", null);

            Map<String, Object> after = null;
            Map<String, Object> before = null;

            String op = normalizeOperationType(operationType);

            if ("c".equals(op)) {
                after = bsonToMap(fullDocument);
            } else if ("u".equals(op)) {
                if (fullDocument != null) {
                    after = bsonToMap(fullDocument);
                } else {
                    // 无 fullDocument 时，把 updateDescription 归一为 $set/$unset，与 Oplog DEFAULT 一致
                    after = updateDescriptionToOperators(
                            changeStreamDocument.getDocument("updateDescription", null));
                }
                if (preImage != null) {
                    before = bsonToMap(preImage);
                } else {
                    // 供 Sink 在 $set/$unset 场景下定位 _id
                    before = bsonToMap(documentKey);
                }
            } else if ("d".equals(op)) {
                if (preImage != null) {
                    before = bsonToMap(preImage);
                } else {
                    before = bsonToMap(documentKey);
                }
            }

            TransferSource source = buildSource(clusterTime);
            long tsMs = clusterTime != null ? clusterTime.getTime() * 1000L : System.currentTimeMillis();

            return TransferEvent.builder()
                    .before(before)
                    .op(op)
                    .after(after)
                    .source(source)
                    .tsMs(tsMs)
                    .build();
        } catch (Exception e) {
            throw new TransferEventConvertException("Failed to convert change stream document", e);
        }
    }

    public TransferEvent createSnapshotEvent(BsonDocument document) {
        try {
            Map<String, Object> after = bsonToMap(document);
            long tsMs = System.currentTimeMillis();

            return TransferEvent.builder()
                    .before(null)
                    .op("r")
                    .after(after)
                    .source(TransferSource.builder()
                            .db(database)
                            .collection(collection)
                            .clusterTime(tsMs)
                            .build())
                    .tsMs(tsMs)
                    .build();
        } catch (Exception e) {
            throw new TransferEventConvertException("Failed to create snapshot event", e);
        }
    }

    /**
     * ChangeStream updateDescription → {@code {$set, $unset}}，与 Oplog / Sink 操作符路径对齐。
     */
    private Map<String, Object> updateDescriptionToOperators(BsonDocument updateDescription) {
        if (updateDescription == null) {
            return null;
        }
        Map<String, Object> after = new HashMap<String, Object>();
        BsonDocument updatedFields = updateDescription.getDocument("updatedFields", null);
        if (updatedFields != null && !updatedFields.isEmpty()) {
            after.put("$set", bsonToMap(updatedFields));
        }
        BsonArray removedFields = updateDescription.containsKey("removedFields")
                && updateDescription.get("removedFields").isArray()
                ? updateDescription.getArray("removedFields")
                : null;
        if (removedFields != null && !removedFields.isEmpty()) {
            Map<String, Object> unset = new HashMap<String, Object>();
            for (BsonValue value : removedFields) {
                if (value != null && value.isString()) {
                    unset.put(value.asString().getValue(), true);
                }
            }
            if (!unset.isEmpty()) {
                after.put("$unset", unset);
            }
        }
        return after.isEmpty() ? null : after;
    }

    private String normalizeOperationType(String operationType) {
        if (operationType == null) {
            return "u";
        }
        switch (operationType.toLowerCase()) {
            case "insert":
                return "c";
            case "update":
                return "u";
            case "replace":
                return "u";
            case "delete":
                return "d";
            case "invalidate":
                return "u";
            default:
                return "u";
        }
    }

    private TransferSource buildSource(BsonTimestamp clusterTime) {
        long clusterTimeMs = clusterTime != null ? clusterTime.getTime() * 1000L : System.currentTimeMillis();

        return TransferSource.builder()
                .db(database)
                .collection(collection)
                .clusterTime(clusterTimeMs)
                .build();
    }

    private Map<String, Object> bsonToMap(BsonDocument document) {
        if (document == null) {
            return null;
        }
        Map<String, Object> result = new HashMap<String, Object>();
        for (Map.Entry<String, BsonValue> entry : document.entrySet()) {
            result.put(entry.getKey(), convertBsonValue(entry.getValue()));
        }
        return result;
    }

    private Object convertBsonValue(BsonValue value) {
        if (value == null || value.isNull()) {
            return null;
        }

        if (value instanceof BsonDocument) {
            return bsonToMap((BsonDocument) value);
        } else if (value instanceof BsonArray) {
            return bsonToList((BsonArray) value);
        } else if (value instanceof BsonString) {
            return ((BsonString) value).getValue();
        } else if (value instanceof BsonInt32) {
            return ((BsonInt32) value).getValue();
        } else if (value instanceof BsonInt64) {
            return ((BsonInt64) value).getValue();
        } else if (value instanceof BsonDouble) {
            return ((BsonDouble) value).getValue();
        } else if (value instanceof BsonBoolean) {
            return ((BsonBoolean) value).getValue();
        } else if (value instanceof BsonObjectId) {
            return ((BsonObjectId) value).getValue().toHexString();
        } else if (value instanceof BsonDateTime) {
            return ((BsonDateTime) value).getValue();
        } else if (value instanceof BsonTimestamp) {
            return ((BsonTimestamp) value).getTime() * 1000L;
        } else if (value instanceof BsonDecimal128) {
            return ((BsonDecimal128) value).getValue().toString();
        } else if (value instanceof BsonBinary) {
            return ((BsonBinary) value).getData();
        } else if (value instanceof BsonRegularExpression) {
            return ((BsonRegularExpression) value).getPattern();
        } else if (value instanceof BsonNull) {
            return null;
        } else if (value instanceof BsonUndefined) {
            return null;
        } else if (value instanceof BsonSymbol) {
            return value.toString();
        } else if (value instanceof BsonJavaScript) {
            return ((BsonJavaScript) value).getCode();
        } else if (value instanceof BsonJavaScriptWithScope) {
            return ((BsonJavaScriptWithScope) value).getCode();
        } else if (value instanceof BsonMaxKey) {
            return null;
        } else if (value instanceof BsonMinKey) {
            return null;
        } else {
            return value.toString();
        }
    }

    private List<Object> bsonToList(BsonArray array) {
        List<Object> result = new ArrayList<Object>();
        for (BsonValue value : array) {
            result.add(convertBsonValue(value));
        }
        return result;
    }
}
