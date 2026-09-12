package com.whaleal.third.mongo.sync.config;

import com.whaleal.third.mongo.sync.error.MongoSyncException;
import org.junit.Assert;
import org.junit.Test;

public class SinkTypeConfigTest {

    @Test
    public void kafkaRequiresBootstrapAndNs() {
        MongoSyncConfig cfg = MongoSyncConfig.builder()
                .sourceUri("mongodb://127.0.0.1:27017/?replicaSet=rs0")
                .sinkUri("127.0.0.1:9092")
                .sinkType(SinkType.KAFKA)
                .mapCollection("demo", "orders")
                .kafkaTopicPrefix("mongo")
                .build();
        Assert.assertEquals(SinkType.KAFKA, cfg.getSinkType());
        Assert.assertEquals("127.0.0.1:9092", cfg.getSinkUri());
        Assert.assertEquals("mongo", cfg.getKafkaTopicPrefix());
    }

    @Test(expected = MongoSyncException.class)
    public void kafkaWithoutSinkUriFails() {
        MongoSyncConfig.builder()
                .sourceUri("mongodb://127.0.0.1:27017/?replicaSet=rs0")
                .sinkType(SinkType.KAFKA)
                .mapCollection("demo", "orders")
                .build();
    }

    @Test
    public void mongodbDefaultUnchanged() {
        MongoSyncConfig cfg = MongoSyncConfig.builder()
                .sourceUri("mongodb://127.0.0.1:27017/?replicaSet=rs0")
                .sinkUri("mongodb://127.0.0.1:27018")
                .mapCollection("demo", "orders")
                .build();
        Assert.assertEquals(SinkType.MONGODB, cfg.getSinkType());
    }

    @Test
    public void parseSinkType() {
        Assert.assertEquals(SinkType.KAFKA, SinkType.parse("kafka"));
        Assert.assertEquals(SinkType.MONGODB, SinkType.parse(null));
    }
}
