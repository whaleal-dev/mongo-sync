package com.whaleal.third.mongo.sync.pipeline;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.whaleal.third.mongo.sink.sdk.MongoSinkClient;
import com.whaleal.third.mongo.sync.cache.SyncCaches;
import com.whaleal.third.mongo.sync.spi.SyncWriteErrorHandler;
import com.whaleal.third.mongo.transfer.model.DdlEvent;
import com.whaleal.third.mongo.transfer.model.DdlType;
import com.whaleal.third.mongo.transfer.model.TransferEvent;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于 LMAX Disruptor 的分桶写入流水线：
 * <ul>
 *   <li>每桶一个 RingBuffer + 单 EventHandler（同桶有序）</li>
 *   <li>同桶同 _id 再次出现时，若前次写入尚未落库则先 flush（对齐 d2t）</li>
 *   <li>DDL：屏障 + 排空后串行 applyDdl</li>
 * </ul>
 */
public final class BucketWritePipeline implements AutoCloseable {

    private final MongoSinkClient sink;
    private final IdBucketRouter router;
    private final SyncCaches caches;
    private final MongoClient sourceClient;
    private final String sourceDatabase;
    private final String sourceCollection;
    private final String sourceNs;
    private final int ddlWaitSeconds;
    private final SyncWriteErrorHandler writeErrorHandler;
    private final List<Disruptor<TransferEventSlot>> disruptors;
    private final List<RingBuffer<TransferEventSlot>> rings;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong inflight = new AtomicLong(0);
    private final AtomicBoolean ddlBarrier = new AtomicBoolean(false);
    private final AtomicLong droppedWhenStopped = new AtomicLong(0);
    private final AtomicBoolean dropWarned = new AtomicBoolean(false);

    public BucketWritePipeline(MongoSinkClient sink,
                               IdBucketRouter router,
                               SyncCaches caches,
                               MongoClient sourceClient,
                               String sourceDatabase,
                               String sourceCollection,
                               String sourceNs,
                               int bucketNum,
                               int ringBufferSize,
                               int ddlWaitSeconds,
                               SyncWriteErrorHandler writeErrorHandler) {
        this.sink = sink;
        this.router = router;
        this.caches = caches;
        this.sourceClient = sourceClient;
        this.sourceDatabase = sourceDatabase;
        this.sourceCollection = sourceCollection;
        this.sourceNs = sourceNs;
        this.ddlWaitSeconds = ddlWaitSeconds;
        this.writeErrorHandler = writeErrorHandler;

        int size = nextPowerOfTwo(Math.max(1024, ringBufferSize));
        this.disruptors = new ArrayList<Disruptor<TransferEventSlot>>(bucketNum);
        this.rings = new ArrayList<RingBuffer<TransferEventSlot>>(bucketNum);

        AtomicInteger threadSeq = new AtomicInteger(1);
        for (int i = 0; i < bucketNum; i++) {
            final int bucketId = i;
            ThreadFactory tf = r -> {
                Thread t = new Thread(r, "mongo-sync-disruptor-" + threadSeq.getAndIncrement());
                t.setDaemon(true);
                return t;
            };
            Disruptor<TransferEventSlot> disruptor = new Disruptor<TransferEventSlot>(
                    new com.lmax.disruptor.EventFactory<TransferEventSlot>() {
                        @Override
                        public TransferEventSlot newInstance() {
                            return new TransferEventSlot();
                        }
                    },
                    size,
                    tf,
                    ProducerType.MULTI,
                    new BlockingWaitStrategy()
            );
            disruptor.handleEventsWith(new BucketHandler(bucketId));
            RingBuffer<TransferEventSlot> ring = disruptor.start();
            disruptors.add(disruptor);
            rings.add(ring);
        }
    }

    public void offer(TransferEvent event) {
        if (event == null) {
            return;
        }
        if (!running.get()) {
            onDroppedWhenStopped();
            return;
        }
        // 与 DDL 屏障协作：必须在计入 inflight 后再次确认 barrier，避免 TOCTOU
        // （barrier 已 true 时 DDL 正 waitDrained，若此时仍 publish 会永远排不空）
        while (running.get()) {
            while (ddlBarrier.get() && running.get()) {
                sleepQuiet(20);
            }
            if (!running.get()) {
                onDroppedWhenStopped();
                return;
            }
            int bucket = router.route(event);
            RingBuffer<TransferEventSlot> ring = rings.get(bucket);
            inflight.incrementAndGet();
            if (ddlBarrier.get() || !running.get()) {
                inflight.decrementAndGet();
                if (!running.get()) {
                    onDroppedWhenStopped();
                    return;
                }
                continue;
            }
            try {
                long seq = ring.next();
                try {
                    TransferEventSlot slot = ring.get(seq);
                    slot.setEvent(event);
                } finally {
                    ring.publish(seq);
                }
                return;
            } catch (Exception e) {
                inflight.decrementAndGet();
                throw e;
            }
        }
    }

    public void applyDdl(DdlEvent event) {
        if (event == null) {
            return;
        }
        caches.lockDdl();
        try {
            ddlBarrier.set(true);
            waitDrained(ddlWaitSeconds);
            sink.flushAndWait();
            sink.applyDdl(event);
            refreshMetadataAfterDdl(event);
        } finally {
            ddlBarrier.set(false);
            caches.unlockDdl();
        }
    }

    /**
     * DDL 落地后刷新运行时元信息：唯一索引分桶策略、Sink ordered。
     */
    private void refreshMetadataAfterDdl(DdlEvent event) {
        if (event.getType() == null) {
            return;
        }
        switch (event.getType()) {
            case CREATE_INDEXES:
            case DROP_INDEXES:
                probeUniqueIndexes(sourceClient, sourceDatabase, sourceCollection, sourceNs, caches);
                sink.setOrdered(caches.hasUniqueIndex(sourceNs));
                System.err.println("[mongo-sync] refreshed unique-index cache after "
                        + event.getType() + " ns=" + sourceNs
                        + " hasUnique=" + caches.hasUniqueIndex(sourceNs));
                break;
            case DROP_COLLECTION:
            case DROP_DATABASE:
                caches.putUniqueIndex(sourceNs, false);
                sink.setOrdered(false);
                break;
            case RENAME_COLLECTION:
                // 目标端已在 Sink retarget；源监视名仍固定。唯一索引缓存按源旧 ns 清掉，避免误用。
                caches.putUniqueIndex(sourceNs, false);
                sink.setOrdered(false);
                break;
            default:
                break;
        }
    }

    public long inflight() {
        return inflight.get();
    }

    /**
     * 尝试排空在途事件并 flush Sink。
     * <p>
     * 全量与增量并行时，增量可能持续入队，{@code inflight} 未必归零；超时后仍 flush，不抛错。
     */
    public void tryDrainAndFlush(int timeoutSeconds) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(Math.max(1, timeoutSeconds));
        while (inflight.get() > 0 && System.nanoTime() < deadline && running.get()) {
            sleepQuiet(50);
        }
        sink.flushAndWait();
    }

    /**
     * 排空分桶在途事件并 flush Sink（要求在途归零，否则超时抛错）。
     * DDL 等必须空窗的场景使用；并行全量+增量请用 {@link #tryDrainAndFlush(int)}。
     */
    public void drainAndFlush(int timeoutSeconds) {
        waitDrained(timeoutSeconds);
        sink.flushAndWait();
    }

    private void waitDrained(int timeoutSeconds) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (inflight.get() > 0) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("timeout waiting CRUD drain before DDL, inflight=" + inflight.get());
            }
            sleepQuiet(50);
        }
    }

    private static void sleepQuiet(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static int nextPowerOfTwo(int value) {
        int v = value;
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;
        return Math.max(1024, v);
    }

    public long droppedWhenStopped() {
        return droppedWhenStopped.get();
    }

    private void onDroppedWhenStopped() {
        long n = droppedWhenStopped.incrementAndGet();
        if (dropWarned.compareAndSet(false, true)) {
            System.err.println("[mongo-sync] pipeline offer dropped while stopped ns=" + sourceNs
                    + " (further drops counted, first warn)");
        } else if (n == 10L || n == 100L || n == 1000L || (n % 10000L) == 0L) {
            System.err.println("[mongo-sync] pipeline offer droppedWhileStopped=" + n + " ns=" + sourceNs);
        }
    }

    @Override
    public void close() {
        running.set(false);
        // 超时也不抛：必须继续 shutdown Disruptor + 最终 flush，避免收尾被跳过
        tryDrainQuiet(Math.max(ddlWaitSeconds, 30));
        for (Disruptor<TransferEventSlot> disruptor : disruptors) {
            try {
                disruptor.shutdown(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                System.err.println("[mongo-sync] disruptor shutdown: " + e.getMessage());
            }
        }
        try {
            sink.flushAndWait();
        } catch (Exception e) {
            System.err.println("[mongo-sync] pipeline final flush: " + e.getMessage());
        }
        long dropped = droppedWhenStopped.get();
        if (dropped > 0L) {
            System.err.println("[mongo-sync] pipeline closed with droppedWhileStopped=" + dropped
                    + " ns=" + sourceNs);
        }
    }

    private void tryDrainQuiet(int timeoutSeconds) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(Math.max(1, timeoutSeconds));
        while (inflight.get() > 0 && System.nanoTime() < deadline) {
            sleepQuiet(50);
        }
        if (inflight.get() > 0) {
            System.err.println("[mongo-sync] pipeline close drain timeout inflight=" + inflight.get()
                    + " ns=" + sourceNs + "; continuing shutdown");
        }
    }

    public static void probeUniqueIndexes(MongoClient sourceClient,
                                          String database,
                                          String collection,
                                          String ns,
                                          SyncCaches caches) {
        // 启动时及 CREATE/DROP_INDEXES DDL 后调用
        if (sourceClient == null || caches == null) {
            return;
        }
        try {
            MongoCollection<Document> coll = sourceClient.getDatabase(database).getCollection(collection);
            boolean hasUnique = false;
            for (Document index : coll.listIndexes()) {
                Object unique = index.get("unique");
                String name = index.getString("name");
                if (Boolean.TRUE.equals(unique) || Integer.valueOf(1).equals(unique)) {
                    if (!"_id_".equals(name)) {
                        hasUnique = true;
                        break;
                    }
                }
            }
            caches.putUniqueIndex(ns, hasUnique);
        } catch (Exception ignored) {
            // 集合已 drop/rename 时探测失败，保守视为无唯一索引（分桶可恢复多桶）
            caches.putUniqueIndex(ns, false);
        }
    }

    private final class BucketHandler implements EventHandler<TransferEventSlot> {

        private final int bucketId;
        /** 本桶尚未确认落库的 _id → 写入序号；随 {@link MongoSinkClient#landedThrough()} 裁剪，有界。 */
        private final Map<String, Long> pendingIds = new HashMap<String, Long>();

        private BucketHandler(int bucketId) {
            this.bucketId = bucketId;
        }

        @Override
        public void onEvent(TransferEventSlot slot, long sequence, boolean endOfBatch) {
            TransferEvent event = slot.getEvent();
            slot.clear();
            try {
                if (event == null) {
                    return;
                }
                String id = IdBucketRouter.extractId(event);
                if (id != null) {
                    Long prevSeq = pendingIds.get(id);
                    if (prevSeq != null && prevSeq.longValue() > sink.landedThrough()) {
                        sink.flushAndWait();
                        prunePendingIds();
                    }
                }
                long seq = sink.write(event);
                if (id != null && seq > 0L) {
                    pendingIds.put(id, Long.valueOf(seq));
                }
                prunePendingIds();
            } catch (Exception e) {
                if (writeErrorHandler != null) {
                    try {
                        writeErrorHandler.onWriteError(bucketId, event, e);
                    } catch (Exception ignored) {
                    }
                } else {
                    System.err.println("[mongo-sync] disruptor bucket=" + bucketId + " write failed: " + e.getMessage());
                }
                try {
                    sink.flushAndWait();
                } catch (Exception ignored) {
                }
                pendingIds.clear();
            } finally {
                inflight.decrementAndGet();
            }
        }

        private void prunePendingIds() {
            long landed = sink.landedThrough();
            if (pendingIds.isEmpty() || landed <= 0L) {
                return;
            }
            Iterator<Map.Entry<String, Long>> it = pendingIds.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Long> entry = it.next();
                if (entry.getValue().longValue() <= landed) {
                    it.remove();
                }
            }
        }
    }
}
