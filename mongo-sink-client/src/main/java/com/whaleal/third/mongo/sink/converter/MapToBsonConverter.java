package com.whaleal.third.mongo.sink.converter;

import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

/**
 * 将 Java Map / 基础类型转为 BsonDocument，便于 Sink 写入。
 */
public final class MapToBsonConverter {

    private MapToBsonConverter() {
    }

    public static BsonDocument toDocument(Map<String, Object> map) {
        if (map == null) {
            return null;
        }
        BsonDocument doc = new BsonDocument();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            doc.put(entry.getKey(), toBsonValue(entry.getValue()));
        }
        return doc;
    }

    @SuppressWarnings("unchecked")
    public static BsonValue toBsonValue(Object value) {
        if (value == null) {
            return BsonNull.VALUE;
        }
        if (value instanceof BsonValue) {
            return (BsonValue) value;
        }
        if (value instanceof BsonDocument) {
            return (BsonDocument) value;
        }
        if (value instanceof Map) {
            return toDocument((Map<String, Object>) value);
        }
        if (value instanceof Collection) {
            BsonArray array = new BsonArray();
            for (Object item : (Collection<?>) value) {
                array.add(toBsonValue(item));
            }
            return array;
        }
        if (value instanceof Object[]) {
            BsonArray array = new BsonArray();
            for (Object item : (Object[]) value) {
                array.add(toBsonValue(item));
            }
            return array;
        }
        if (value instanceof String) {
            String s = (String) value;
            if (ObjectId.isValid(s) && s.length() == 24) {
                // 仅当字段看起来像 ObjectId 时不自动转换；保持字符串更安全
                return new BsonString(s);
            }
            return new BsonString(s);
        }
        if (value instanceof ObjectId) {
            return new BsonObjectId((ObjectId) value);
        }
        if (value instanceof Integer) {
            return new BsonInt32((Integer) value);
        }
        if (value instanceof Long) {
            return new BsonInt64((Long) value);
        }
        if (value instanceof Double) {
            return new BsonDouble((Double) value);
        }
        if (value instanceof Float) {
            return new BsonDouble(((Float) value).doubleValue());
        }
        if (value instanceof Boolean) {
            return new BsonBoolean((Boolean) value);
        }
        if (value instanceof Date) {
            return new BsonDateTime(((Date) value).getTime());
        }
        if (value instanceof byte[]) {
            return new org.bson.BsonBinary((byte[]) value);
        }
        return new BsonString(String.valueOf(value));
    }

    /**
     * 尝试将 after/before 中的 id 字段还原为 ObjectId（若为 24 位 hex）。
     */
    public static BsonDocument toDocumentWithObjectId(Map<String, Object> map, String idField) {
        BsonDocument doc = toDocument(map);
        if (doc == null || idField == null || !doc.containsKey(idField)) {
            return doc;
        }
        BsonValue idValue = doc.get(idField);
        if (idValue != null && idValue.isString()) {
            String hex = idValue.asString().getValue();
            if (ObjectId.isValid(hex)) {
                doc.put(idField, new BsonObjectId(new ObjectId(hex)));
            }
        }
        return doc;
    }
}
