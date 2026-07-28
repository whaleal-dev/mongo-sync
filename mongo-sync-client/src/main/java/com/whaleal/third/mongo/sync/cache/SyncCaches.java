package com.whaleal.third.mongo.sync.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 对齐 d2t 的缓存/锁：
 * <ul>
 *   <li>ns 入队锁：同 ns 串行 offer（不可过期，避免持锁期间被淘汰导致双入）</li>
 *   <li>DDL 全局锁：DDL 与 CRUD 互斥</li>
 *   <li>唯一索引标记：启动探测；CREATE/DROP_INDEXES 后再探测（不过期）</li>
 * </ul>
 */
public final class SyncCaches {

    private final ConcurrentHashMap<String, AtomicBoolean> nsParseLocks =
            new ConcurrentHashMap<String, AtomicBoolean>();
    private final LoadingCache<String, Boolean> uniqueIndexCache;
    private final ReentrantLock ddlLock = new ReentrantLock(true);

    public SyncCaches(long nsLockExpireMinutes) {
        // nsLockExpireMinutes 保留兼容构造参数，ns 锁改为不过期的 ConcurrentHashMap
        this.uniqueIndexCache = Caffeine.newBuilder()
                .maximumSize(10_000)
                .build(key -> Boolean.FALSE);
    }

    public boolean tryLockNs(String ns) {
        AtomicBoolean flag = nsParseLocks.computeIfAbsent(ns, k -> new AtomicBoolean(false));
        return flag.compareAndSet(false, true);
    }

    public void unlockNs(String ns) {
        AtomicBoolean flag = nsParseLocks.get(ns);
        if (flag != null) {
            flag.set(false);
        }
    }

    public void lockDdl() {
        ddlLock.lock();
    }

    public void unlockDdl() {
        ddlLock.unlock();
    }

    public boolean tryLockDdl() {
        return ddlLock.tryLock();
    }

    public void putUniqueIndex(String ns, boolean hasUnique) {
        uniqueIndexCache.put(ns, hasUnique);
    }

    public boolean hasUniqueIndex(String ns) {
        Boolean v = uniqueIndexCache.getIfPresent(ns);
        return v != null && v;
    }

    public LoadingCache<String, Boolean> uniqueIndexCache() {
        return uniqueIndexCache;
    }
}
