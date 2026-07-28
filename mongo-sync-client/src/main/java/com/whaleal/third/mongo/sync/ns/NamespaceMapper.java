package com.whaleal.third.mongo.sync.ns;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 源 ns → 目标 ns 映射（对齐 MongoShake {@code transform.namespace}）。
 * <p>
 * 映射键值均为 {@code db.collection}；未命中时默认同源同名。
 */
public final class NamespaceMapper {

    private final Map<String, String> sourceToTarget;

    private NamespaceMapper(Map<String, String> sourceToTarget) {
        this.sourceToTarget = sourceToTarget;
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
        String targetNs = sourceToTarget.get(sourceNs);
        if (targetNs == null) {
            return new NsPair(sourceDatabase, sourceCollection, sourceDatabase, sourceCollection);
        }
        int dot = targetNs.indexOf('.');
        return new NsPair(
                sourceDatabase,
                sourceCollection,
                targetNs.substring(0, dot),
                targetNs.substring(dot + 1));
    }

    public Map<String, String> asMap() {
        return sourceToTarget;
    }

    public static final class NsPair {
        public final String sourceDatabase;
        public final String sourceCollection;
        public final String targetDatabase;
        public final String targetCollection;

        public NsPair(String sourceDatabase, String sourceCollection,
                      String targetDatabase, String targetCollection) {
            this.sourceDatabase = sourceDatabase;
            this.sourceCollection = sourceCollection;
            this.targetDatabase = targetDatabase;
            this.targetCollection = targetCollection;
        }

        public String sourceNs() {
            return sourceDatabase + "." + sourceCollection;
        }

        public String targetNs() {
            return targetDatabase + "." + targetCollection;
        }
    }
}
