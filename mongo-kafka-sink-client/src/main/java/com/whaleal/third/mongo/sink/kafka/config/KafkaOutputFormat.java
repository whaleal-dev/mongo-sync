package com.whaleal.third.mongo.sink.kafka.config;

/**
 * Kafka value 编码，对齐 mongo-kafka {@code output.format.value}。
 */
public enum KafkaOutputFormat {

    /** Change Stream 文档的 Extended/STRICT JSON 字符串（默认，兼容 mongo-kafka JSON）。 */
    JSON,

    /** Change Stream 文档的 BSON 字节。 */
    BSON;

    public static KafkaOutputFormat parse(String value) {
        if (value == null || value.trim().isEmpty()) {
            return JSON;
        }
        return KafkaOutputFormat.valueOf(value.trim().toUpperCase());
    }
}
