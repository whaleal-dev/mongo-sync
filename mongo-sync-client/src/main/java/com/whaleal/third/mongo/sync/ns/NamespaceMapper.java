package com.whaleal.third.mongo.sync.ns;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 源 ns → Sink ns 映射（对齐 MongoShake {@code transform.namespace}）。
 * <p>
 * 映射键值均为 {@code db.collection}；未命中时默认同源同名。
 */
public final class NamespaceMapper {

    private final Map<String, String> sourceToSink;

    private NamespaceMapper(Map<String, String> sourceToSink) {
        this.sourceToSink = sourceToSink;
    }

    public static NamespaceMapper identity() {
        return new NamespaceMapper(Collections.<String, String>emptyMap());
    }

    /**
     * @param mappingSemicolon 形如 {@code srcDb.srcColl:tgtDb.tgtColl;a.b:c.d}
     */
    public static NamespaceMapper of(String mappingSemicolon) {
        if (mappingSemicolon == null || mappingSemicolon.trim().isEmpty()) {
            return identity();
        }
        Map<String, String> map = new LinkedHashMap<String, String>();
        String[] pairs = mappingSemicolon.split(";");
        for (String pair : pairs) {
            if (pair == null || pair.trim().isEmpty()) {
                continue;
            }
            String[] kv = pair.trim().split(":", 2);
            if (kv.length != 2) {
                throw new IllegalArgumentException(
                        "invalid namespace transform entry (expect srcDb.srcColl:tgtDb.tgtColl): " + pair);
            }
            String from = kv[0].trim();
            String to = kv[1].trim();
            if (!from.contains(".") || !to.contains(".")) {
                throw new IllegalArgumentException(
                        "namespace transform must be db.collection on both sides: " + pair);
            }
            map.put(from, to);
        }
        return new NamespaceMapper(Collections.unmodifiableMap(map));
    }

    public NsPair map(String sourceDatabase, String sourceCollection) {
        String sourceNs = sourceDatabase + "." + sourceCollection;
        String sinkNs = sourceToSink.get(sourceNs);
        if (sinkNs == null) {
            return new NsPair(sourceDatabase, sourceCollection, sourceDatabase, sourceCollection);
        }
        int dot = sinkNs.indexOf('.');
        return new NsPair(
                sourceDatabase,
                sourceCollection,
                sinkNs.substring(0, dot),
                sinkNs.substring(dot + 1));
    }

    public Map<String, String> asMap() {
        return sourceToSink;
    }

    public static final class NsPair {
        public final String sourceDatabase;
        public final String sourceCollection;
        public final String sinkDatabase;
        public final String sinkCollection;

        public NsPair(String sourceDatabase, String sourceCollection,
                      String sinkDatabase, String sinkCollection) {
            this.sourceDatabase = sourceDatabase;
            this.sourceCollection = sourceCollection;
            this.sinkDatabase = sinkDatabase;
            this.sinkCollection = sinkCollection;
        }

        public String sourceNs() {
            return sourceDatabase + "." + sourceCollection;
        }

        public String sinkNs() {
            return sinkDatabase + "." + sinkCollection;
        }
    }
}
