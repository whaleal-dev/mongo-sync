package com.whaleal.third.mongo.sink.model;

import com.mongodb.bulk.BulkWriteResult;

public class SinkWriteResult {

    private final int insertedCount;
    private final int matchedCount;
    private final int modifiedCount;
    private final int deletedCount;
    private final int upsertedCount;

    public SinkWriteResult(int insertedCount, int matchedCount, int modifiedCount,
                           int deletedCount, int upsertedCount) {
        this.insertedCount = insertedCount;
        this.matchedCount = matchedCount;
        this.modifiedCount = modifiedCount;
        this.deletedCount = deletedCount;
        this.upsertedCount = upsertedCount;
    }

    public static SinkWriteResult empty() {
        return new SinkWriteResult(0, 0, 0, 0, 0);
    }

    public static SinkWriteResult from(BulkWriteResult result) {
        if (result == null) {
            return empty();
        }
        return new SinkWriteResult(
                result.getInsertedCount(),
                result.getMatchedCount(),
                result.getModifiedCount(),
                result.getDeletedCount(),
                result.getUpserts() == null ? 0 : result.getUpserts().size()
        );
    }

    public int getInsertedCount() {
        return insertedCount;
    }

    public int getMatchedCount() {
        return matchedCount;
    }

    public int getModifiedCount() {
        return modifiedCount;
    }

    public int getDeletedCount() {
        return deletedCount;
    }

    public int getUpsertedCount() {
        return upsertedCount;
    }

    public SinkWriteResult merge(SinkWriteResult other) {
        if (other == null) {
            return this;
        }
        return new SinkWriteResult(
                this.insertedCount + other.insertedCount,
                this.matchedCount + other.matchedCount,
                this.modifiedCount + other.modifiedCount,
                this.deletedCount + other.deletedCount,
                this.upsertedCount + other.upsertedCount
        );
    }
}
