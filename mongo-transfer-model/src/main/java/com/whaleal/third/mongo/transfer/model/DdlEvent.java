package com.whaleal.third.mongo.transfer.model;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

/**
 * 通用 DDL 传输事件。Sink 按 {@link DdlType} + {@link #command} 落地，不关心来自 Oplog 还是 ChangeStream。
 */
public class DdlEvent {

    private DdlType type;
    private String database;
    private String collection;
    private BsonDocument command;
    private BsonTimestamp ts;
    private Long wallTimeMs;
    /** 可选元数据：源端 Mongo 版本字符串 */
    private String mongoVersion;
    /** 可选元数据：Oplog 格式版本名（V1/V2/V3），非 Oplog 可为空 */
    private String oplogFormat;

    public DdlEvent() {
    }

    public static Builder builder() {
        return new Builder();
    }

    public DdlType getType() {
        return type;
    }

    public String getDatabase() {
        return database;
    }

    public String getCollection() {
        return collection;
    }

    public BsonDocument getCommand() {
        return command;
    }

    public BsonTimestamp getTs() {
        return ts;
    }

    public Long getWallTimeMs() {
        return wallTimeMs;
    }

    public String getMongoVersion() {
        return mongoVersion;
    }

    public String getOplogFormat() {
        return oplogFormat;
    }

    public static class Builder {
        private DdlType type;
        private String database;
        private String collection;
        private BsonDocument command;
        private BsonTimestamp ts;
        private Long wallTimeMs;
        private String mongoVersion;
        private String oplogFormat;

        public Builder type(DdlType type) {
            this.type = type;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder collection(String collection) {
            this.collection = collection;
            return this;
        }

        public Builder command(BsonDocument command) {
            this.command = command;
            return this;
        }

        public Builder ts(BsonTimestamp ts) {
            this.ts = ts;
            return this;
        }

        public Builder wallTimeMs(Long wallTimeMs) {
            this.wallTimeMs = wallTimeMs;
            return this;
        }

        public Builder mongoVersion(String mongoVersion) {
            this.mongoVersion = mongoVersion;
            return this;
        }

        public Builder oplogFormat(String oplogFormat) {
            this.oplogFormat = oplogFormat;
            return this;
        }

        public DdlEvent build() {
            DdlEvent event = new DdlEvent();
            event.type = this.type;
            event.database = this.database;
            event.collection = this.collection;
            event.command = this.command;
            event.ts = this.ts;
            event.wallTimeMs = this.wallTimeMs;
            event.mongoVersion = this.mongoVersion;
            event.oplogFormat = this.oplogFormat;
            return event;
        }
    }
}
