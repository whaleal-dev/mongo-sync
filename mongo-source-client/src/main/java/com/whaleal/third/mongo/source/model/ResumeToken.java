package com.whaleal.third.mongo.source.model;

import org.bson.BsonDocument;

/**
 * ChangeStream 位点（resume token）。
 * <p>
 * Oplog 位点请使用 {@link OplogOffset}，二者不可混用。
 */
public class ResumeToken {

    private final BsonDocument token;

    private ResumeToken(BsonDocument token) {
        this.token = token;
    }

    public static ResumeToken fromBson(BsonDocument token) {
        return new ResumeToken(token);
    }

    public static ResumeToken empty() {
        return new ResumeToken(null);
    }

    public boolean isEmpty() {
        return token == null || token.isEmpty();
    }

    public BsonDocument getToken() {
        return token;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResumeToken that = (ResumeToken) o;
        return token != null ? token.equals(that.token) : that.token == null;
    }

    @Override
    public int hashCode() {
        return token != null ? token.hashCode() : 0;
    }
}
