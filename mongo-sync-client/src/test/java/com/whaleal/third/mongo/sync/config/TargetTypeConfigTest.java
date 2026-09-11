package com.whaleal.third.mongo.sync.config;

import com.whaleal.third.mongo.sync.error.MongoSyncException;
import org.junit.Assert;
import org.junit.Test;

public class TargetTypeConfigTest {

    @Test
    public void kafkaRequiresBootstrapAndNs() {
        MongoSyncConfig cfg = MongoSyncConfig.builder()
                .sourceUri("mongodb://127.0.0.1:27017/?replicaSet=rs0")
                .targetUri("127.0.0.1:9092")
                .targetType(TargetType.KAFKA)
                .mapCollection("demo", "orders")
                .kafkaTopicPrefix("mongo")
                .build();
        Assert.assertEquals(TargetType.KAFKA, cfg.getTargetType());
        Assert.assertEquals("127.0.0.1:9092", cfg.getTargetUri());
        Assert.assertEquals("mongo", cfg.getKafkaTopicPrefix());
    }

    @Test(expected = MongoSyncException.class)
    public void kafkaWithoutTargetUriFails() {
        MongoSyncConfig.builder()
                .sourceUri("mongodb://127.0.0.1:27017/?replicaSet=rs0")
                .targetType(TargetType.KAFKA)
                .mapCollection("demo", "orders")
                .build();
    }

    @Test
    public void mongodbDefaultUnchanged() {
        MongoSyncConfig cfg = MongoSyncConfig.builder()
                .sourceUri("mongodb://127.0.0.1:27017/?replicaSet=rs0")
                .targetUri("mongodb://127.0.0.1:27018")
                .mapCollection("demo", "orders")
                .build();
        Assert.assertEquals(TargetType.MONGODB, cfg.getTargetType());
    }

    @Test
    public void parseTargetType() {
        Assert.assertEquals(TargetType.KAFKA, TargetType.parse("kafka"));
        Assert.assertEquals(TargetType.MONGODB, TargetType.parse(null));
    }
}
