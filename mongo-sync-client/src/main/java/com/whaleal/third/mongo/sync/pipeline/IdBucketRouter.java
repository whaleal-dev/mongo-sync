package com.whaleal.third.mongo.sync.pipeline;

import com.whaleal.third.mongo.sync.cache.SyncCaches;
import com.whaleal.third.mongo.transfer.model.TransferEvent;

import java.util.Map;

/**
 * 按文档 _id 哈希分桶（对齐 d2t：{@code Math.abs(id.hashCode() % bucketNum)}）。
 * 有唯一索引时强制 bucket=0，避免跨桶乱序破坏唯一约束。
 */
public final class IdBucketRouter {

    private final int bucketNum;
    private final boolean forceSingleBucketOnUniqueIndex;
    private final SyncCaches caches;
    private final String ns;

    public IdBucketRouter(int bucketNum,
                          boolean forceSingleBucketOnUniqueIndex,
                          SyncCaches caches,
                          String ns) {
        this.bucketNum = Math.max(1, bucketNum);
        this.forceSingleBucketOnUniqueIndex = forceSingleBucketOnUniqueIndex;
        this.caches = caches;
        this.ns = ns;
    }

    public int route(TransferEvent event) {
        if (forceSingleBucketOnUniqueIndex && caches.hasUniqueIndex(ns)) {
            return 0;
        }
        String id = extractId(event);
        if (id == null) {
            return 0;
        }
        return Math.floorMod(id.hashCode(), bucketNum);
    }

    public static String extractId(TransferEvent event) {
        if (event == null) {
            return null;
        }
        Object id = firstId(event.getAfter());
        if (id == null) {
            id = firstId(event.getBefore());
        }
        return id == null ? null : String.valueOf(id);
    }

    private static Object firstId(Map<String, Object> doc) {
        if (doc == null) {
            return null;
        }
        return doc.get("_id");
    }
}
