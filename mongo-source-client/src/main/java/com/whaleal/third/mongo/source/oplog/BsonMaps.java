package com.whaleal.third.mongo.source.oplog;

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

public final class BsonMaps {

    private BsonMaps() {
    }

    public static Map<String, Object> toMap(BsonDocument document) {
        if (document == null) {
            return null;
        }
        Map<String, Object> result = new HashMap<String, Object>();
        for (Map.Entry<String, BsonValue> entry : document.entrySet()) {
            result.put(entry.getKey(), toJava(entry.getValue()));
        }
        return result;
    }

    public static Object toJava(BsonValue value) {
        if (value == null || value.isNull()) {
            return null;
        }
        if (value instanceof BsonDocument) {
            return toMap((BsonDocument) value);
        } else if (value instanceof BsonArray) {
            List<Object> list = new ArrayList<Object>();
            for (BsonValue item : (BsonArray) value) {
                list.add(toJava(item));
            }
            return list;
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
        } else if (value instanceof BsonNull || value instanceof BsonUndefined
                || value instanceof BsonMaxKey || value instanceof BsonMinKey) {
            return null;
        } else if (value instanceof BsonSymbol) {
            return value.toString();
        } else if (value instanceof BsonJavaScript) {
            return ((BsonJavaScript) value).getCode();
        } else if (value instanceof BsonJavaScriptWithScope) {
            return ((BsonJavaScriptWithScope) value).getCode();
        }
        return value.toString();
    }
}
