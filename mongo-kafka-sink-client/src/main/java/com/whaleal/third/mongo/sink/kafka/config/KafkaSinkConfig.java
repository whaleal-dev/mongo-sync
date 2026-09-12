package com.whaleal.third.mongo.sink.kafka.config;

import org.apache.kafka.clients.producer.Producer;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Kafka Sink 配置。{@code sink.uri} 视为 bootstrap servers（可带 {@code kafka://} 前缀）。
 */
public final class KafkaSinkConfig {

    public static final String DEFAULT_TOPIC_SEPARATOR = ".";
    public static final String DEFAULT_ACKS = "all";
    public static final String DEFAULT_COMPRESSION = "lz4";
    public static final String DEFAULT_CLIENT_ID = "mongo-sync";
    public static final int DEFAULT_LINGER_MS = 5;
    public static final int DEFAULT_BATCH_SIZE_BYTES = 16384;

    private String bootstrapServers;
    private String database;
    private String collection;
    /** 固定 topic；非空时忽略 prefix/db/coll 拼接。 */
    private String topic;
    private String topicPrefix = "";
    private String topicSeparator = DEFAULT_TOPIC_SEPARATOR;
    private String topicSuffix = "";
    private KafkaOutputFormat outputFormat = KafkaOutputFormat.JSON;
    private boolean publishDdl = true;
    private String acks = DEFAULT_ACKS;
    private int lingerMs = DEFAULT_LINGER_MS;
    private int batchSizeBytes = DEFAULT_BATCH_SIZE_BYTES;
    private String compressionType = DEFAULT_COMPRESSION;
    private String clientId = DEFAULT_CLIENT_ID;
    private Map<String, String> extraProducerProperties = Collections.emptyMap();
    /** 测试可注入；生产为 null，由 Writer 自建 Producer。 */
    private Producer<String, byte[]> producer;
    private boolean closeProducerOnClose = true;

    private KafkaSinkConfig() {
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getDatabase() {
        return database;
    }

    public String getCollection() {
        return collection;
    }

    public String getTopic() {
        return topic;
    }

    public String getTopicPrefix() {
        return topicPrefix;
    }

    public String getTopicSeparator() {
        return topicSeparator;
    }

    public String getTopicSuffix() {
        return topicSuffix;
    }

    public KafkaOutputFormat getOutputFormat() {
        return outputFormat == null ? KafkaOutputFormat.JSON : outputFormat;
    }

    public boolean isPublishDdl() {
        return publishDdl;
    }

    public String getAcks() {
        return acks;
    }

    public int getLingerMs() {
        return lingerMs;
    }

    public int getBatchSizeBytes() {
        return batchSizeBytes;
    }

    public String getCompressionType() {
        return compressionType;
    }

    public String getClientId() {
        return clientId;
    }

    public Map<String, String> getExtraProducerProperties() {
        return extraProducerProperties;
    }

    public Producer<String, byte[]> getProducer() {
        return producer;
    }

    public boolean isCloseProducerOnClose() {
        return closeProducerOnClose;
    }

    /**
     * 去掉 {@code kafka://} / {@code kafka:} 前缀，得到 bootstrap.servers。
     */
    public static String normalizeBootstrap(String uri) {
        if (uri == null) {
            return null;
        }
        String s = uri.trim();
        if (s.regionMatches(true, 0, "kafka://", 0, 8)) {
            return s.substring(8).trim();
        }
        if (s.regionMatches(true, 0, "kafka:", 0, 6)) {
            return s.substring(6).trim();
        }
        return s;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final KafkaSinkConfig c = new KafkaSinkConfig();

        public Builder bootstrapServers(String bootstrapServers) {
            c.bootstrapServers = normalizeBootstrap(bootstrapServers);
            return this;
        }

        public Builder database(String database) {
            c.database = database;
            return this;
        }

        public Builder collection(String collection) {
            c.collection = collection;
            return this;
        }

        public Builder topic(String topic) {
            c.topic = topic;
            return this;
        }

        public Builder topicPrefix(String topicPrefix) {
            c.topicPrefix = topicPrefix == null ? "" : topicPrefix;
            return this;
        }

        public Builder topicSeparator(String topicSeparator) {
            c.topicSeparator = (topicSeparator == null || topicSeparator.isEmpty())
                    ? DEFAULT_TOPIC_SEPARATOR : topicSeparator;
            return this;
        }

        public Builder topicSuffix(String topicSuffix) {
            c.topicSuffix = topicSuffix == null ? "" : topicSuffix;
            return this;
        }

        public Builder outputFormat(KafkaOutputFormat outputFormat) {
            c.outputFormat = outputFormat == null ? KafkaOutputFormat.JSON : outputFormat;
            return this;
        }

        public Builder publishDdl(boolean publishDdl) {
            c.publishDdl = publishDdl;
            return this;
        }

        public Builder acks(String acks) {
            c.acks = (acks == null || acks.trim().isEmpty()) ? DEFAULT_ACKS : acks.trim();
            return this;
        }

        public Builder lingerMs(int lingerMs) {
            c.lingerMs = lingerMs < 0 ? DEFAULT_LINGER_MS : lingerMs;
            return this;
        }

        public Builder batchSizeBytes(int batchSizeBytes) {
            c.batchSizeBytes = batchSizeBytes > 0 ? batchSizeBytes : DEFAULT_BATCH_SIZE_BYTES;
            return this;
        }

        public Builder compressionType(String compressionType) {
            c.compressionType = (compressionType == null || compressionType.trim().isEmpty())
                    ? DEFAULT_COMPRESSION : compressionType.trim();
            return this;
        }

        public Builder clientId(String clientId) {
            c.clientId = (clientId == null || clientId.trim().isEmpty())
                    ? DEFAULT_CLIENT_ID : clientId.trim();
            return this;
        }

        public Builder extraProducerProperties(Map<String, String> extraProducerProperties) {
            if (extraProducerProperties == null || extraProducerProperties.isEmpty()) {
                c.extraProducerProperties = Collections.emptyMap();
            } else {
                c.extraProducerProperties = Collections.unmodifiableMap(
                        new LinkedHashMap<String, String>(extraProducerProperties));
            }
            return this;
        }

        public Builder producer(Producer<String, byte[]> producer) {
            c.producer = producer;
            c.closeProducerOnClose = false;
            return this;
        }

        public Builder closeProducerOnClose(boolean closeProducerOnClose) {
            c.closeProducerOnClose = closeProducerOnClose;
            return this;
        }

        public KafkaSinkConfig build() {
            if (c.producer == null && (c.bootstrapServers == null || c.bootstrapServers.isEmpty())) {
                throw new IllegalArgumentException("bootstrapServers or producer is required");
            }
            if (c.database == null || c.database.trim().isEmpty()) {
                throw new IllegalArgumentException("database is required");
            }
            if (c.collection == null || c.collection.trim().isEmpty()) {
                throw new IllegalArgumentException("collection is required");
            }
            return c;
        }
    }
}
