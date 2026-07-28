package com.whaleal.third.mongo.source.model;

import org.bson.BsonTimestamp;

/**
 * Oplog 位点（{@code local.oplog.rs} 的 {@code ts} 时间戳）。
 * <p>
 * ChangeStream 位点请使用 {@link ResumeToken}，二者不可混用。
 */
public class OplogOffset {

    private final BsonTimestamp timestamp;

    private OplogOffset(BsonTimestamp timestamp) {
        this.timestamp = timestamp;
    }

    public static OplogOffset of(BsonTimestamp timestamp) {
        return new OplogOffset(timestamp);
    }

    public static OplogOffset empty() {
        return new OplogOffset(null);
    }

    public boolean isEmpty() {
        return timestamp == null;
    }

    public BsonTimestamp getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OplogOffset that = (OplogOffset) o;
        return timestamp != null ? timestamp.equals(that.timestamp) : that.timestamp == null;
    }

    @Override
    public int hashCode() {
        return timestamp != null ? timestamp.hashCode() : 0;
    }

    @Override
    public String toString() {
        if (timestamp == null) {
            return "OplogOffset{empty}";
        }
        return "OplogOffset{t=" + timestamp.getTime() + ", i=" + timestamp.getInc() + "}";
    }
}
