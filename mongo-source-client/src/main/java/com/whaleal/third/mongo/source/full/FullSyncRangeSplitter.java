package com.whaleal.third.mongo.source.full;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 大表全量拆分（对齐 d2t {@code SpliceNsData}）：
 * <ul>
 *   <li>按 {@code _id} 的 BSON 类型分别取 min/max</li>
 *   <li>按 collStats.avgObjSize 与任务体积（MB）估算每段文档数，再 {@code skip} 切段</li>
 *   <li>末段 {@code isMax=true} 使用闭区间 {@code [min,max]}，其余 {@code [min,max)}</li>
 * </ul>
 */
public final class FullSyncRangeSplitter {

    /** 常见可作主键的 BSON type 码（跳过 array=4，大表上 $type:array 易卡死，对齐 d2t）。 */
    private static final int[] ID_BSON_TYPES = {
            1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, -1, 127
    };

    private FullSyncRangeSplitter() {
    }

    /**
     * 一个全量读任务的 _id 区间。
     */
    public static final class IdRange {
        /** 含下界 */
        public final Object minInclusive;
        /** 上界：{@link #isMax} 为 true 时含，否则不含 */
        public final Object max;
        public final boolean isMax;

        public IdRange(Object minInclusive, Object max, boolean isMax) {
            this.minInclusive = minInclusive;
            this.max = max;
            this.isMax = isMax;
        }

        /** 全表（不按 _id 过滤）。 */
        public static IdRange all() {
            return new IdRange(null, null, true);
        }

        public Bson toFilter() {
            if (minInclusive == null && max == null) {
                return new BsonDocument();
            }
            if (minInclusive == null) {
                return isMax ? Filters.lte("_id", max) : Filters.lt("_id", max);
            }
            if (max == null) {
                return Filters.gte("_id", minInclusive);
            }
            if (isMax) {
                return Filters.and(Filters.gte("_id", minInclusive), Filters.lte("_id", max));
            }
            return Filters.and(Filters.gte("_id", minInclusive), Filters.lt("_id", max));
        }

        @Override
        public String toString() {
            String right = isMax ? "]" : ")";
            return "[" + minInclusive + ", " + max + right;
        }
    }

    /**
     * @param taskMbSize 单任务目标数据量（MB），对齐 d2t {@code SpliceNsData.mbSize}，默认 32
     * @return 切分后的区间列表；空表返回空列表
     */
    public static List<IdRange> split(MongoDatabase database,
                                      MongoCollection<BsonDocument> collection,
                                      int taskMbSize) {
        long count = collection.estimatedDocumentCount();
        if (count <= 0) {
            return Collections.emptyList();
        }

        int rangeSize = computeRangeDocCount(database, collection.getNamespace().getCollectionName(), taskMbSize);
        long avgObjSize = getAvgObjSize(database, collection.getNamespace().getCollectionName());
        System.err.println("[mongo-source] full-sync estimate count=" + count
                + " rangeDocs=" + rangeSize + " avgObjSize=" + avgObjSize
                + " taskMb=" + Math.max(1, taskMbSize));

        Map<Integer, IdRange> typeBounds = discoverIdTypeBounds(collection);
        if (typeBounds.isEmpty()) {
            // 无 $type 命中时回退：整表一个区间
            System.err.println("[mongo-source] full-sync no typed _id bounds, use single range");
            return Collections.singletonList(IdRange.all());
        }

        List<IdRange> ranges = new ArrayList<IdRange>();
        for (Map.Entry<Integer, IdRange> e : typeBounds.entrySet()) {
            IdRange remaining = e.getValue();
            Object cursorMin = remaining.minInclusive;
            Object typeMax = remaining.max;
            while (cursorMin != null) {
                IdRange piece = splitOne(collection, cursorMin, typeMax, rangeSize);
                ranges.add(piece);
                if (piece.isMax) {
                    break;
                }
                cursorMin = piece.max;
            }
        }
        System.err.println("[mongo-source] full-sync split into " + ranges.size() + " range task(s)");
        return ranges;
    }

    private static IdRange splitOne(MongoCollection<BsonDocument> collection,
                                    Object minInclusive,
                                    Object typeMax,
                                    int rangeSize) {
        BsonDocument boundary = null;
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                boundary = collection.find(Filters.gte("_id", minInclusive))
                        .projection(Projections.include("_id"))
                        .sort(Sorts.ascending("_id"))
                        .skip(Math.max(1, rangeSize))
                        .limit(1)
                        .first();
                break;
            } catch (Exception e) {
                if (attempt == 3) {
                    System.err.println("[mongo-source] full-sync split skip failed: " + e.getMessage());
                }
            }
        }
        if (boundary != null && boundary.containsKey("_id")) {
            return new IdRange(minInclusive, boundary.get("_id"), false);
        }
        return new IdRange(minInclusive, typeMax, true);
    }

    private static Map<Integer, IdRange> discoverIdTypeBounds(MongoCollection<BsonDocument> collection) {
        Map<Integer, IdRange> map = new LinkedHashMap<Integer, IdRange>();
        for (int type : ID_BSON_TYPES) {
            try {
                Document typeFilter = new Document("_id", new Document("$type", type));
                BsonDocument probe = collection.find(typeFilter)
                        .projection(Projections.include("_id"))
                        .limit(1)
                        .first();
                if (probe == null) {
                    continue;
                }
                IdRange bounds = minMaxForType(collection, type);
                if (bounds != null) {
                    map.put(type, bounds);
                    System.err.println("[mongo-source] full-sync _id type=" + type
                            + " min=" + bounds.minInclusive + " max=" + bounds.max);
                }
            } catch (Exception e) {
                System.err.println("[mongo-source] full-sync probe _id type=" + type
                        + " skipped: " + e.getMessage());
            }
        }
        return map;
    }

    private static IdRange minMaxForType(MongoCollection<BsonDocument> collection, int type) {
        Document typeFilter = new Document("_id", new Document("$type", type));
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                BsonDocument minDoc = collection.find(typeFilter)
                        .projection(Projections.include("_id"))
                        .sort(Sorts.ascending("_id"))
                        .limit(1)
                        .first();
                BsonDocument maxDoc = collection.find(typeFilter)
                        .projection(Projections.include("_id"))
                        .sort(Sorts.descending("_id"))
                        .limit(1)
                        .first();
                if (minDoc == null || maxDoc == null
                        || !minDoc.containsKey("_id") || !maxDoc.containsKey("_id")) {
                    return null;
                }
                return new IdRange(minDoc.get("_id"), maxDoc.get("_id"), true);
            } catch (Exception e) {
                if (attempt == 3) {
                    System.err.println("[mongo-source] full-sync min/max type=" + type
                            + " failed: " + e.getMessage());
                }
            }
        }
        return null;
    }

    /**
     * 按任务体积估算每段文档数（对齐 d2t {@code computeBatchSize}）。
     */
    static int computeRangeDocCount(MongoDatabase database, String collectionName, int taskMbSize) {
        long avg = getAvgObjSize(database, collectionName);
        int mb = Math.max(1, taskMbSize);
        int batch = Math.round(mb * 1024L * 1024L / (avg + 0.0F));
        if (batch >= 1_024_000) {
            return 1_024_000;
        }
        if (batch <= 1024) {
            return 1024;
        }
        return batch;
    }

    static long getAvgObjSize(MongoDatabase database, String collectionName) {
        try {
            Document stats = database.runCommand(new Document("collStats", collectionName));
            if (!stats.containsKey("avgObjSize")) {
                return 1024L;
            }
            return Long.parseLong(String.valueOf(stats.get("avgObjSize")));
        } catch (Exception e) {
            return 1024L;
        }
    }
}
