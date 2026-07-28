package com.whaleal.third.mongo.source.oplog;

import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.Map;

/**
 * 将 MongoDB 5.0+ oplog update 的 {@code diff} 归一为 {@code $set/$unset}（对齐 d2t 逻辑）。
 */
public final class OplogDiffNormalizer {

    private OplogDiffNormalizer() {
    }

    public static BsonDocument normalizeUpdateDocument(BsonDocument o) {
        if (o == null) {
            return null;
        }
        BsonDocument normalized = new BsonDocument();
        for (Map.Entry<String, BsonValue> entry : o.entrySet()) {
            if ("$v".equals(entry.getKey())) {
                continue;
            }
            normalized.put(entry.getKey(), entry.getValue());
        }

        if (normalized.containsKey("diff") && !normalized.containsKey("_id")) {
            BsonDocument setDoc = new BsonDocument();
            BsonDocument unsetDoc = new BsonDocument();
            BsonDocument diff = normalized.getDocument("diff");
            for (Map.Entry<String, BsonValue> entry : diff.entrySet()) {
                parseDiff(setDoc, unsetDoc, entry.getKey(), entry.getValue(), "");
            }
            BsonDocument up = new BsonDocument();
            if (!setDoc.isEmpty()) {
                up.put("$set", setDoc);
            }
            if (!unsetDoc.isEmpty()) {
                up.put("$unset", unsetDoc);
            }
            return up;
        }
        return normalized;
    }

    private static void parseDiff(BsonDocument setDoc, BsonDocument unsetDoc,
                                  String key, BsonValue value, String preKey) {
        if (key.startsWith("d")) {
            if (key.length() == 1 && value.isDocument()) {
                BsonDocument document = value.asDocument();
                if (preKey.length() == 0) {
                    for (Map.Entry<String, BsonValue> e : document.entrySet()) {
                        unsetDoc.put(e.getKey(), e.getValue());
                    }
                } else {
                    for (Map.Entry<String, BsonValue> e : document.entrySet()) {
                        unsetDoc.put(preKey + "." + e.getKey(), e.getValue());
                    }
                }
            }
        } else if (key.startsWith("i")) {
            if (key.length() == 1 && value.isDocument()) {
                BsonDocument document = value.asDocument();
                if (preKey.length() == 0) {
                    for (Map.Entry<String, BsonValue> e : document.entrySet()) {
                        setDoc.put(e.getKey(), e.getValue());
                    }
                } else {
                    for (Map.Entry<String, BsonValue> e : document.entrySet()) {
                        setDoc.put(preKey + "." + e.getKey(), e.getValue());
                    }
                }
            }
        } else if (key.startsWith("u")) {
            if (key.length() >= 2) {
                setDoc.put(preKey + "." + key.substring(1), value);
            } else if (key.length() == 1 && value.isDocument()) {
                BsonDocument document = value.asDocument();
                if (preKey.length() == 0) {
                    for (Map.Entry<String, BsonValue> e : document.entrySet()) {
                        setDoc.put(e.getKey(), e.getValue());
                    }
                } else {
                    for (Map.Entry<String, BsonValue> e : document.entrySet()) {
                        setDoc.put(preKey + "." + e.getKey(), e.getValue());
                    }
                }
            }
        } else if (key.startsWith("s") && value.isDocument()) {
            BsonDocument document = value.asDocument();
            String field = key.substring(1);
            String preKeyTemp = preKey.length() == 0 ? field : preKey + "." + field;
            for (Map.Entry<String, BsonValue> entry : document.entrySet()) {
                if ("a".equals(entry.getKey())) {
                    continue;
                }
                parseDiff(setDoc, unsetDoc, entry.getKey(), entry.getValue(), preKeyTemp);
            }
        }
    }
}
