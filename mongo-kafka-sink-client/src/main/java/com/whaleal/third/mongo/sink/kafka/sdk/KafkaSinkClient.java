package com.whaleal.third.mongo.sink.kafka.sdk;

import com.whaleal.third.mongo.sink.kafka.config.KafkaOutputFormat;
import com.whaleal.third.mongo.sink.kafka.config.KafkaSinkConfig;
import com.whaleal.third.mongo.sink.kafka.writer.KafkaSinkWriter;
import com.whaleal.third.mongo.transfer.model.DdlEvent;
import com.whaleal.third.mongo.transfer.model.TransferEvent;
import com.whaleal.third.mongo.transfer.spi.TransferSink;
import org.apache.kafka.clients.producer.Producer;

import java.util.Map;

/**
 * Kafka Sink：把 {@link TransferEvent} / {@link DdlEvent} 写成 mongo-kafka 兼容的 Change Stream 消息。
 */
public final class KafkaSinkClient implements TransferSink {

    private final KafkaSinkConfig config;
    private final KafkaSinkWriter writer;

    private KafkaSinkClient(KafkaSinkConfig config) {
        this.config = config;
        this.writer = new KafkaSinkWriter(config);
    }

    public static Builder builder() {
        return new Builder();
    }

    public KafkaSinkConfig getConfig() {
        return config;
    }

    @Override
    public long write(TransferEvent event) {
        return writer.write(event);
    }

    @Override
    public long landedThrough() {
        return writer.landedThrough();
    }

    @Override
    public void applyDdl(DdlEvent event) {
        writer.applyDdl(event);
    }

    @Override
    public void setOrdered(boolean ordered) {
        writer.setOrdered(ordered);
    }

    public boolean isOrdered() {
        return writer.isOrdered();
    }

    public String getActiveCollection() {
        return writer.getActiveCollection();
    }

    @Override
    public void flushAndWait() {
        writer.flushAndWait();
    }

    @Override
    public void close() {
        writer.close();
    }

    public static final class Builder {
        private final KafkaSinkConfig.Builder inner = KafkaSinkConfig.builder();

        public Builder bootstrapServers(String bootstrapServers) {
            inner.bootstrapServers(bootstrapServers);
            return this;
        }

        public Builder database(String database) {
            inner.database(database);
            return this;
        }

        public Builder collection(String collection) {
            inner.collection(collection);
            return this;
        }

        public Builder topic(String topic) {
            inner.topic(topic);
            return this;
        }

        public Builder topicPrefix(String topicPrefix) {
            inner.topicPrefix(topicPrefix);
            return this;
        }

        public Builder topicSeparator(String topicSeparator) {
            inner.topicSeparator(topicSeparator);
            return this;
        }

        public Builder topicSuffix(String topicSuffix) {
            inner.topicSuffix(topicSuffix);
            return this;
        }

        public Builder outputFormat(KafkaOutputFormat outputFormat) {
            inner.outputFormat(outputFormat);
            return this;
        }

        public Builder publishDdl(boolean publishDdl) {
            inner.publishDdl(publishDdl);
            return this;
        }

        public Builder acks(String acks) {
            inner.acks(acks);
            return this;
        }

        public Builder lingerMs(int lingerMs) {
            inner.lingerMs(lingerMs);
            return this;
        }

        public Builder batchSizeBytes(int batchSizeBytes) {
            inner.batchSizeBytes(batchSizeBytes);
            return this;
        }

        public Builder compressionType(String compressionType) {
            inner.compressionType(compressionType);
            return this;
        }

        public Builder clientId(String clientId) {
            inner.clientId(clientId);
            return this;
        }

        public Builder extraProducerProperties(Map<String, String> extraProducerProperties) {
            inner.extraProducerProperties(extraProducerProperties);
            return this;
        }

        public Builder producer(Producer<String, byte[]> producer) {
            inner.producer(producer);
            return this;
        }

        public KafkaSinkClient build() {
            return new KafkaSinkClient(inner.build());
        }
    }
}
