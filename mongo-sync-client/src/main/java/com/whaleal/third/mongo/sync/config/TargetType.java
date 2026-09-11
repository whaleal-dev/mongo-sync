package com.whaleal.third.mongo.sync.config;

/**
 * 同步目标形态。
 */
public enum TargetType {

    /** 写入 MongoDB / 协议兼容库（默认）。 */
    MONGODB,

    /**
     * 写入 Kafka。消息格式对齐 mongo-kafka Source（Change Stream JSON/BSON），
     * 下游可用 mongo-kafka Sink 的 {@code ChangeStreamHandler} 消费。
     */
    KAFKA;

    public boolean isKafka() {
        return this == KAFKA;
    }

    public boolean isMongodb() {
        return this == MONGODB;
    }

    public static TargetType parse(String value) {
        if (value == null || value.trim().isEmpty()) {
            return MONGODB;
        }
        return TargetType.valueOf(value.trim().toUpperCase());
    }
}
