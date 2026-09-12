package com.whaleal.third.mongo.sync.verify;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 源/Sink 集合数据比对（count / _id / 全文）。
 */
public final class DataVerifier {

    private final MongoClient sourceClient;
    private final MongoClient sinkClient;
    private final VerifyMode mode;
    private final Set<String> ignoreFields;
    private final int maxSampleDiffs;
    private final int batchSize;

    public DataVerifier(MongoClient sourceClient,
                        MongoClient sinkClient,
                        VerifyMode mode,
                        Set<String> ignoreFields,
                        int maxSampleDiffs,
                        int batchSize) {
        this.sourceClient = sourceClient;
        this.sinkClient = sinkClient;
        this.mode = mode == null ? VerifyMode.FULL : mode;
        this.ignoreFields = ignoreFields == null
                ? new HashSet<String>()
                : new HashSet<String>(ignoreFields);
        this.maxSampleDiffs = maxSampleDiffs > 0 ? maxSampleDiffs : 50;
        this.batchSize = batchSize > 0 ? batchSize : 500;
    }

    public CollectionVerifyReport verify(String sourceDatabase,
                                         String sourceCollection,
                                         String sinkDatabase,
                                         String sinkCollection) {
        String sourceNs = sourceDatabase + "." + sourceCollection;
        String sinkNs = sinkDatabase + "." + sinkCollection;
        MongoCollection<Document> source =
                sourceClient.getDatabase(sourceDatabase).getCollection(sourceCollection);
        MongoCollection<Document> sink =
                sinkClient.getDatabase(sinkDatabase).getCollection(sinkCollection);

        long sourceCount = source.countDocuments();
        long sinkCount = sink.countDocuments();
        List<String> samples = new ArrayList<String>();

        if (mode == VerifyMode.COUNT) {
            if (sourceCount != sinkCount) {
                samples.add("count differs source=" + sourceCount + " sink=" + sinkCount);
            }
            return new CollectionVerifyReport(
                    sourceNs, sinkNs, sourceCount, sinkCount, 0, 0, 0, 0, samples);
        }

        long missingOnSink = 0;
        long missingOnSource = 0;
        long contentMismatch = 0;
        long compared = 0;

        Bson idProj = Projections.include("_id");
        try (MongoCursor<Document> sc = source.find().projection(idProj).sort(Sorts.ascending("_id")).batchSize(batchSize).iterator();
             MongoCursor<Document> tc = sink.find().projection(idProj).sort(Sorts.ascending("_id")).batchSize(batchSize).iterator()) {

            Document s = sc.hasNext() ? sc.next() : null;
            Document t = tc.hasNext() ? tc.next() : null;

            while (s != null || t != null) {
                if (s == null) {
                    missingOnSource++;
                    addSample(samples, "extraOnSink _id=" + t.get("_id"));
                    t = tc.hasNext() ? tc.next() : null;
                    continue;
                }
                if (t == null) {
                    missingOnSink++;
                    addSample(samples, "missingOnSink _id=" + s.get("_id"));
                    s = sc.hasNext() ? sc.next() : null;
                    continue;
                }
                int cmp = compareId(s.get("_id"), t.get("_id"));
                if (cmp < 0) {
                    missingOnSink++;
                    addSample(samples, "missingOnSink _id=" + s.get("_id"));
                    s = sc.hasNext() ? sc.next() : null;
                } else if (cmp > 0) {
                    missingOnSource++;
                    addSample(samples, "extraOnSink _id=" + t.get("_id"));
                    t = tc.hasNext() ? tc.next() : null;
                } else {
                    // same _id
                    if (mode == VerifyMode.FULL) {
                        compared++;
                        Document fullS = source.find(Filters.eq("_id", s.get("_id"))).first();
                        Document fullT = sink.find(Filters.eq("_id", t.get("_id"))).first();
                        if (!documentsEqual(fullS, fullT)) {
                            contentMismatch++;
                            addSample(samples, "mismatch _id=" + s.get("_id"));
                        }
                    } else {
                        compared++;
                    }
                    s = sc.hasNext() ? sc.next() : null;
                    t = tc.hasNext() ? tc.next() : null;
                }
            }
        }

        return new CollectionVerifyReport(
                sourceNs, sinkNs, sourceCount, sinkCount,
                missingOnSink, missingOnSource, contentMismatch, compared, samples);
    }

    private void addSample(List<String> samples, String msg) {
        if (samples.size() < maxSampleDiffs) {
            samples.add(msg);
        }
    }

    @SuppressWarnings("unchecked")
    private static int compareId(Object a, Object b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return -1;
        }
        if (b == null) {
            return 1;
        }
        if (a instanceof Comparable && a.getClass().isInstance(b)) {
            return ((Comparable) a).compareTo(b);
        }
        return String.valueOf(a).compareTo(String.valueOf(b));
    }

    private boolean documentsEqual(Document source, Document sink) {
        if (source == null && sink == null) {
            return true;
        }
        if (source == null || sink == null) {
            return false;
        }
        Document s = strip(source);
        Document t = strip(sink);
        return deepEquals(s, t);
    }

    private Document strip(Document doc) {
        Document copy = new Document(doc);
        for (String f : ignoreFields) {
            copy.remove(f);
        }
        return copy;
    }

    @SuppressWarnings("unchecked")
    private static boolean deepEquals(Object a, Object b) {
        if (a == b) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        if (a instanceof Document && b instanceof Document) {
            Document da = (Document) a;
            Document db = (Document) b;
            if (da.size() != db.size()) {
                return false;
            }
            for (Map.Entry<String, Object> e : da.entrySet()) {
                if (!deepEquals(e.getValue(), db.get(e.getKey()))) {
                    return false;
                }
            }
            return true;
        }
        if (a instanceof List && b instanceof List) {
            List<?> la = (List<?>) a;
            List<?> lb = (List<?>) b;
            if (la.size() != lb.size()) {
                return false;
            }
            for (int i = 0; i < la.size(); i++) {
                if (!deepEquals(la.get(i), lb.get(i))) {
                    return false;
                }
            }
            return true;
        }
        if (a instanceof byte[] && b instanceof byte[]) {
            return Arrays.equals((byte[]) a, (byte[]) b);
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() == ((Number) b).doubleValue();
        }
        return a.equals(b);
    }

    public static Set<String> parseIgnoreFields(String semicolon) {
        Set<String> set = new LinkedHashSet<String>();
        if (semicolon == null || semicolon.trim().isEmpty()) {
            return set;
        }
        for (String p : semicolon.split(";")) {
            if (p != null && !p.trim().isEmpty()) {
                set.add(p.trim());
            }
        }
        return set;
    }
}
