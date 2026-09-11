package com.whaleal.third.mongo.sink.kafka.topic;

import com.whaleal.third.mongo.sink.kafka.config.KafkaSinkConfig;

/**
 * Topic 命名对齐 mongo-kafka {@code DefaultTopicMapper}：
 * {@code [prefix + sep] + db + [sep + coll] + [sep + suffix]}。
 * 配置了固定 {@code kafka.topic} 时全部事件走该 topic。
 */
public final class KafkaTopicMapper {

    private final String fixedTopic;
    private final String prefix;
    private final String separator;
    private final String suffix;

    public KafkaTopicMapper(KafkaSinkConfig config) {
        this.fixedTopic = trimToNull(config.getTopic());
        this.separator = config.getTopicSeparator() == null
                ? KafkaSinkConfig.DEFAULT_TOPIC_SEPARATOR : config.getTopicSeparator();
        String rawPrefix = config.getTopicPrefix() == null ? "" : config.getTopicPrefix();
        String rawSuffix = config.getTopicSuffix() == null ? "" : config.getTopicSuffix();
        this.prefix = rawPrefix.isEmpty() ? "" : rawPrefix + separator;
        this.suffix = rawSuffix.isEmpty() ? "" : separator + rawSuffix;
    }

    public String topic(String database, String collection) {
        if (fixedTopic != null) {
            return fixedTopic;
        }
        String db = database == null ? "" : database;
        String coll = collection == null ? "" : collection;
        String undecorated = coll.isEmpty() ? db : db + separator + coll;
        return prefix + undecorated + suffix;
    }

    public boolean isFixedTopic() {
        return fixedTopic != null;
    }

    private static String trimToNull(String s) {
        if (s == null) {
            return null;
        }
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }
}
