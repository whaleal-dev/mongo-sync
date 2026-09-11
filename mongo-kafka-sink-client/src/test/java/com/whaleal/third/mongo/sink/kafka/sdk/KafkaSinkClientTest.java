package com.whaleal.third.mongo.sink.kafka.sdk;

import com.whaleal.third.mongo.sink.kafka.config.KafkaOutputFormat;
import com.whaleal.third.mongo.transfer.model.TransferEvent;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class KafkaSinkClientTest {

    @Test
    public void writePublishesChangeStreamJson() {
        MockProducer<String, byte[]> mock = new MockProducer<String, byte[]>(
                true, new StringSerializer(), new ByteArraySerializer());
        KafkaSinkClient client = KafkaSinkClient.builder()
                .producer(mock)
                .database("demo")
                .collection("orders")
                .topicPrefix("mongo")
                .outputFormat(KafkaOutputFormat.JSON)
                .build();
        try {
            Map<String, Object> after = new HashMap<String, Object>();
            after.put("_id", 1);
            after.put("n", "a");
            long seq = client.write(TransferEvent.builder().op("c").after(after).build());
            Assert.assertTrue(seq > 0L);
            client.flushAndWait();
            Assert.assertEquals(1, mock.history().size());
            ProducerRecord<String, byte[]> rec = mock.history().get(0);
            Assert.assertEquals("mongo.demo.orders", rec.topic());
            String value = new String(rec.value(), StandardCharsets.UTF_8);
            Assert.assertTrue(value.contains("\"operationType\""));
            Assert.assertTrue(value.contains("insert"));
            Assert.assertTrue(client.landedThrough() >= seq);
        } finally {
            client.close();
        }
    }
}
