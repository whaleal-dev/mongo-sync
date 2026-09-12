package com.whaleal.third.mongo.sync.sink;

import com.mongodb.client.MongoClient;
import com.whaleal.third.mongo.sink.kafka.config.KafkaOutputFormat;
import com.whaleal.third.mongo.sink.kafka.sdk.KafkaSinkClient;
import com.whaleal.third.mongo.sink.sdk.MongoSinkClient;
import com.whaleal.third.mongo.sync.config.MongoSyncConfig;
import com.whaleal.third.mongo.sync.config.SinkType;
import com.whaleal.third.mongo.transfer.model.DdlEvent;
import com.whaleal.third.mongo.transfer.model.TransferEvent;
import com.whaleal.third.mongo.transfer.spi.TransferSink;

/**
 * 按 {@link SinkType} 创建 Sink。
 */
public final class SinkFactory {

    private SinkFactory() {
    }

    public static TransferSink create(MongoSyncConfig config,
                                      MongoClient sinkMongoClient,
                                      boolean orderedWrite) {
        if (config.getSinkType() == SinkType.KAFKA) {
            return KafkaSinkClient.builder()
                    .bootstrapServers(config.getSinkUri())
                    .database(config.getSinkDatabase())
                    .collection(config.getSinkCollection())
                    .topic(config.getKafkaTopic())
                    .topicPrefix(config.getKafkaTopicPrefix())
                    .topicSeparator(config.getKafkaTopicSeparator())
                    .topicSuffix(config.getKafkaTopicSuffix())
                    .outputFormat(config.getKafkaOutputFormat() == null
                            ? KafkaOutputFormat.JSON : config.getKafkaOutputFormat())
                    .publishDdl(config.isKafkaPublishDdl())
                    .acks(config.getKafkaAcks())
                    .lingerMs(config.getKafkaLingerMs())
                    .batchSizeBytes(config.getKafkaBatchSizeBytes())
                    .compressionType(config.getKafkaCompressionType())
                    .clientId(config.getKafkaClientId())
                    .extraProducerProperties(config.getKafkaProducerProperties())
                    .build();
        }
        MongoSinkClient mongo = MongoSinkClient.builder()
                .mongoClient(sinkMongoClient)
                .closeMongoClientOnClose(false)
                .database(config.getSinkDatabase())
                .collection(config.getSinkCollection())
                .writeMode(config.getWriteMode())
                .onConflict(config.getOnConflict())
                .batchSize(config.getSinkBatchSize())
                .writerThreads(config.getSinkWriterThreads())
                .ordered(orderedWrite)
                .build();
        return new MongoSinkAdapter(mongo);
    }

    static final class MongoSinkAdapter implements TransferSink {
        private final MongoSinkClient client;

        MongoSinkAdapter(MongoSinkClient client) {
            this.client = client;
        }

        @Override
        public long write(TransferEvent event) {
            return client.write(event);
        }

        @Override
        public long landedThrough() {
            return client.landedThrough();
        }

        @Override
        public void applyDdl(DdlEvent event) {
            client.applyDdl(event);
        }

        @Override
        public void setOrdered(boolean ordered) {
            client.setOrdered(ordered);
        }

        @Override
        public void flushAndWait() {
            client.flushAndWait();
        }

        @Override
        public void close() {
            client.close();
        }
    }
}
