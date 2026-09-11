package com.whaleal.third.mongo.sink.kafka.writer;

import com.whaleal.third.mongo.sink.exception.SinkWriteException;
import com.whaleal.third.mongo.sink.kafka.codec.ChangeStreamDocumentEncoder;
import com.whaleal.third.mongo.sink.kafka.config.KafkaOutputFormat;
import com.whaleal.third.mongo.sink.kafka.config.KafkaSinkConfig;
import com.whaleal.third.mongo.sink.kafka.topic.KafkaTopicMapper;
import com.whaleal.third.mongo.transfer.model.DdlEvent;
import com.whaleal.third.mongo.transfer.model.DdlType;
import com.whaleal.third.mongo.transfer.model.TransferEvent;
import com.whaleal.third.mongo.transfer.spi.TransferSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 将 Change Stream 文档异步写入 Kafka；{@link #landedThrough()} 以 produce ack 为准。
 */
public final class KafkaSinkWriter implements TransferSink {

    private final KafkaSinkConfig config;
    private final KafkaTopicMapper topicMapper;
    private final Producer<String, byte[]> producer;
    private final boolean ownsProducer;
    private final AtomicReference<String> activeDatabase;
    private final AtomicReference<String> activeCollection;
    private final AtomicBoolean ordered = new AtomicBoolean(false);
    private final Object lock = new Object();
    private long enqueueSeq;
    private final ConcurrentSkipListMap<Long, Long> pending = new ConcurrentSkipListMap<Long, Long>();
    private final AtomicLong submittedThrough = new AtomicLong(0);
    private final AtomicReference<Throwable> asyncError = new AtomicReference<Throwable>();

    public KafkaSinkWriter(KafkaSinkConfig config) {
        this.config = config;
        this.topicMapper = new KafkaTopicMapper(config);
        this.activeDatabase = new AtomicReference<String>(config.getDatabase());
        this.activeCollection = new AtomicReference<String>(config.getCollection());
        if (config.getProducer() != null) {
            this.producer = config.getProducer();
            this.ownsProducer = config.isCloseProducerOnClose();
        } else {
            this.producer = new KafkaProducer<String, byte[]>(producerProperties(config));
            this.ownsProducer = true;
        }
    }

    @Override
    public long write(TransferEvent event) {
        checkAsyncError();
        BsonDocument change = ChangeStreamDocumentEncoder.encode(
                event, activeDatabase.get(), activeCollection.get());
        if (change == null) {
            return 0L;
        }
        return send(change);
    }

    @Override
    public void applyDdl(DdlEvent event) {
        flushAndWait();
        if (event == null) {
            return;
        }
        if (config.isPublishDdl()) {
            BsonDocument change = ChangeStreamDocumentEncoder.encodeDdl(
                    event, activeDatabase.get(), activeCollection.get());
            if (change != null) {
                send(change);
                flushAndWait();
            }
        }
        retargetAfterDdl(event);
    }

    @Override
    public void setOrdered(boolean orderedWrite) {
        this.ordered.set(orderedWrite);
    }

    public boolean isOrdered() {
        return ordered.get();
    }

    public String getActiveCollection() {
        return activeCollection.get();
    }

    @Override
    public long landedThrough() {
        long submitted = submittedThrough.get();
        Map.Entry<Long, Long> oldest = pending.firstEntry();
        if (oldest == null) {
            return submitted;
        }
        return oldest.getValue() - 1;
    }

    @Override
    public void flushAndWait() {
        checkAsyncError();
        producer.flush();
        checkAsyncError();
    }

    @Override
    public void close() {
        try {
            flushAndWait();
        } finally {
            if (ownsProducer && producer != null) {
                try {
                    producer.close(Duration.ofSeconds(30));
                } catch (Exception e) {
                    System.err.println("[mongo-kafka-sink] producer close: " + e.getMessage());
                }
            }
        }
    }

    private long send(BsonDocument change) {
        checkAsyncError();
        String topic = topicMapper.topic(activeDatabase.get(), activeCollection.get());
        String key = ChangeStreamDocumentEncoder.keyJson(change);
        byte[] value = encodeValue(change);
        ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topic, key, value);

        final long seq;
        synchronized (lock) {
            seq = ++enqueueSeq;
            pending.put(seq, seq);
            submittedThrough.set(seq);
        }
        try {
            producer.send(record, (metadata, exception) -> {
                pending.remove(seq);
                if (exception != null) {
                    asyncError.compareAndSet(null, exception);
                }
            });
        } catch (RuntimeException e) {
            pending.remove(seq);
            throw new SinkWriteException("kafka send failed, topic=" + topic, e);
        }
        return seq;
    }

    private byte[] encodeValue(BsonDocument change) {
        if (config.getOutputFormat() == KafkaOutputFormat.BSON) {
            return ChangeStreamDocumentEncoder.toBsonBytes(change);
        }
        return ChangeStreamDocumentEncoder.toJsonBytes(change);
    }

    private void retargetAfterDdl(DdlEvent event) {
        if (event.getType() != DdlType.RENAME_COLLECTION || topicMapper.isFixedTopic()) {
            return;
        }
        if (event.getCommand() == null || !event.getCommand().containsKey("to")) {
            return;
        }
        BsonValue to = event.getCommand().get("to");
        String toDb = activeDatabase.get();
        String toColl = null;
        if (to != null && to.isString()) {
            String v = to.asString().getValue();
            int dot = v.indexOf('.');
            if (dot > 0) {
                toDb = v.substring(0, dot);
                toColl = v.substring(dot + 1);
            } else {
                toColl = v;
            }
        } else if (to != null && to.isDocument()) {
            BsonDocument toDoc = to.asDocument();
            if (toDoc.containsKey("db") && toDoc.get("db").isString()) {
                toDb = toDoc.getString("db").getValue();
            }
            if (toDoc.containsKey("coll") && toDoc.get("coll").isString()) {
                toColl = toDoc.getString("coll").getValue();
            }
        }
        if (toColl == null || toColl.isEmpty()) {
            return;
        }
        activeDatabase.set(toDb);
        activeCollection.set(toColl);
        System.err.println("[mongo-kafka-sink] retarget topic ns after rename -> "
                + toDb + "." + toColl);
    }

    private void checkAsyncError() {
        Throwable err = asyncError.get();
        if (err != null) {
            throw new SinkWriteException("kafka produce previously failed", err);
        }
    }

    static Properties producerProperties(KafkaSinkConfig config) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, config.getAcks());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.LINGER_MS_CONFIG, Integer.toString(config.getLingerMs()));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(config.getBatchSizeBytes()));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getCompressionType());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, config.getClientId());
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        Map<String, String> extra = config.getExtraProducerProperties();
        if (extra != null) {
            for (Map.Entry<String, String> e : extra.entrySet()) {
                if (e.getKey() != null && e.getValue() != null) {
                    props.put(e.getKey(), e.getValue());
                }
            }
        }
        return props;
    }
}
