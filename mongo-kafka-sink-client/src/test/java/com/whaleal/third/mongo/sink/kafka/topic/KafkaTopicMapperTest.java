package com.whaleal.third.mongo.sink.kafka.topic;

import com.whaleal.third.mongo.sink.kafka.config.KafkaSinkConfig;
import org.junit.Assert;
import org.junit.Test;

public class KafkaTopicMapperTest {

    @Test
    public void defaultIsDbSeparatorColl() {
        KafkaTopicMapper mapper = new KafkaTopicMapper(base().build());
        Assert.assertEquals("demo.orders", mapper.topic("demo", "orders"));
    }

    @Test
    public void prefixAndSuffixMatchMongoKafka() {
        KafkaTopicMapper mapper = new KafkaTopicMapper(base()
                .topicPrefix("mongo")
                .topicSuffix("cdc")
                .build());
        Assert.assertEquals("mongo.demo.orders.cdc", mapper.topic("demo", "orders"));
    }

    @Test
    public void fixedTopicWins() {
        KafkaTopicMapper mapper = new KafkaTopicMapper(base()
                .topic("all-changes")
                .topicPrefix("mongo")
                .build());
        Assert.assertEquals("all-changes", mapper.topic("demo", "orders"));
        Assert.assertTrue(mapper.isFixedTopic());
    }

    private static KafkaSinkConfig.Builder base() {
        return KafkaSinkConfig.builder()
                .bootstrapServers("localhost:9092")
                .database("demo")
                .collection("orders");
    }
}
