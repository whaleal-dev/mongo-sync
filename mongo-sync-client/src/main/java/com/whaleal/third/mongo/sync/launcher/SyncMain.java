package com.whaleal.third.mongo.sync.launcher;

import com.whaleal.third.mongo.sink.config.OnConflict;
import com.whaleal.third.mongo.sink.config.WriteMode;
import com.whaleal.third.mongo.source.config.CaptureMode;
import com.whaleal.third.mongo.source.config.MongoSourceConfig;
import com.whaleal.third.mongo.source.config.SyncMode;
import com.whaleal.third.mongo.sync.config.MongoMultiSyncConfig;
import com.whaleal.third.mongo.sync.config.MongoSyncConfig;
import com.whaleal.third.mongo.sync.sdk.MigrationProgress;
import com.whaleal.third.mongo.sync.sdk.MongoMultiSyncClient;
import com.whaleal.third.mongo.sync.sdk.MongoSyncClient;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 同步进程入口（配置文件启动）。
 * <p>
 * 用法：
 * <pre>
 *   java -cp ... com.whaleal.third.mongo.sync.launcher.SyncMain conf/mongo-sync.properties
 *   java -cp ... SyncMain --config conf/mongo-sync.properties
 * </pre>
 * <p>
 * 有 {@code namespace.white}/{@code namespace.black} 时走 {@link MongoMultiSyncClient}，否则单表
 * {@link MongoSyncClient}。Ctrl+C / SIGTERM 触发优雅停止。
 */
public final class SyncMain {

    private SyncMain() {
    }

    public static void main(String[] args) {
        int code = 2;
        try {
            code = run(args);
        } catch (Exception e) {
            System.err.println("[mongo-sync] ERROR " + e.getMessage());
            e.printStackTrace(System.err);
            code = 2;
        }
        System.exit(code);
    }

    static int run(String[] args) throws Exception {
        Properties props = loadArgs(args);
        String sourceUri = req(props, "source.uri");
        String targetUri = req(props, "target.uri");

        boolean multi = hasText(props.getProperty("namespace.white"))
                || hasText(props.getProperty("namespace.black"));
        final boolean autoCommitWhenReady = bool(props, "commit.when.ready", false);
        final int progressLogSeconds = integer(props, "progress.log.interval.seconds", 10);

        final AtomicReference<AutoCloseable> clientRef = new AtomicReference<AutoCloseable>();
        final CountDownLatch stop = new CountDownLatch(1);
        final ScheduledExecutorService progressExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "mongo-sync-progress");
            t.setDaemon(true);
            return t;
        });

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                System.err.println("[mongo-sync] shutting down...");
                AutoCloseable c = clientRef.get();
                if (c != null) {
                    try {
                        c.close();
                    } catch (Exception e) {
                        System.err.println("[mongo-sync] close error: " + e.getMessage());
                    }
                }
                progressExecutor.shutdownNow();
                stop.countDown();
            }
        }, "mongo-sync-shutdown"));

        if (multi) {
            MongoMultiSyncClient multiClient = MongoMultiSyncClient.create(buildMulti(props, sourceUri, targetUri));
            clientRef.set(multiClient);
            System.err.println("[mongo-sync] starting MULTI-SYNC white="
                    + props.getProperty("namespace.white")
                    + " black=" + props.getProperty("namespace.black")
                    + " syncMode=" + get(props, "sync.mode", "FULL_AND_INCREMENTAL")
                    + " capture=" + get(props, "capture.mode", "AUTO"));
            multiClient.start();
            scheduleProgress(progressExecutor, progressLogSeconds, stop, autoCommitWhenReady, multiClient);
        } else {
            MongoSyncClient sync = MongoSyncClient.create(buildSingle(props, sourceUri, targetUri));
            clientRef.set(sync);
            System.err.println("[mongo-sync] starting SYNC ns="
                    + req(props, "source.database") + "." + req(props, "source.collection")
                    + " → " + get(props, "target.database", props.getProperty("source.database"))
                    + "." + get(props, "target.collection", props.getProperty("source.collection"))
                    + " syncMode=" + get(props, "sync.mode", "FULL_AND_INCREMENTAL")
                    + " capture=" + get(props, "capture.mode", "AUTO")
                    + " topology=" + sync.getSourceTopology()
                    + " resolvedCapture=" + sync.getResolvedCaptureMode());
            sync.start();
            scheduleProgress(progressExecutor, progressLogSeconds, stop, autoCommitWhenReady, sync);
        }

        System.err.println("[mongo-sync] running (Ctrl+C to stop)");
        stop.await();
        System.err.println("[mongo-sync] stopped");
        return 0;
    }

    private static void scheduleProgress(ScheduledExecutorService executor,
                                         int intervalSeconds,
                                         CountDownLatch stop,
                                         boolean autoCommitWhenReady,
                                         final MongoSyncClient sync) {
        if (intervalSeconds <= 0) {
            return;
        }
        executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    MigrationProgress p = sync.progress();
                    System.err.println("[mongo-sync] progress " + p);
                    if (autoCommitWhenReady && p.isCanCommit()) {
                        MigrationProgress committed = sync.commit();
                        System.err.println("[mongo-sync] committed " + committed);
                        stop.countDown();
                    }
                } catch (Exception e) {
                    System.err.println("[mongo-sync] progress/commit error: " + e.getMessage());
                }
            }
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    private static void scheduleProgress(ScheduledExecutorService executor,
                                         int intervalSeconds,
                                         CountDownLatch stop,
                                         boolean autoCommitWhenReady,
                                         final MongoMultiSyncClient sync) {
        if (intervalSeconds <= 0) {
            return;
        }
        executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    MigrationProgress p = sync.progress();
                    System.err.println("[mongo-sync] progress " + p);
                    if (autoCommitWhenReady && p.isCanCommit()) {
                        MigrationProgress committed = sync.commit();
                        System.err.println("[mongo-sync] committed " + committed);
                        stop.countDown();
                    }
                } catch (Exception e) {
                    System.err.println("[mongo-sync] progress/commit error: " + e.getMessage());
                }
            }
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    private static MongoSyncConfig.Builder buildSingle(Properties props, String sourceUri, String targetUri) {
        String sdb = req(props, "source.database");
        String scoll = req(props, "source.collection");
        String tdb = get(props, "target.database", sdb);
        String tcoll = get(props, "target.collection", scoll);

        MongoSyncConfig.Builder b = MongoSyncClient.builder()
                .sourceUri(sourceUri)
                .targetUri(targetUri)
                .sourceDatabase(sdb)
                .sourceCollection(scoll)
                .targetDatabase(tdb)
                .targetCollection(tcoll)
                .captureMode(CaptureMode.valueOf(get(props, "capture.mode", "AUTO").toUpperCase()))
                .syncMode(SyncMode.valueOf(get(props, "sync.mode", "FULL_AND_INCREMENTAL").toUpperCase()))
                .fullDocument(MongoSourceConfig.FullDocumentMode.valueOf(
                        get(props, "full.document", "DEFAULT").toUpperCase()))
                .enablePreImage(bool(props, "enable.pre.image", false))
                .includeFromMigrate(bool(props, "include.from.migrate", false))
                .writeMode(WriteMode.valueOf(get(props, "write.mode", "UPSERT").toUpperCase()))
                .onConflict(OnConflict.valueOf(get(props, "on.conflict", "FAIL").toUpperCase()))
                .targetBatchSize(integer(props, "target.batch.size", 1000))
                .targetWriterThreads(integer(props, "target.writer.threads", 8))
                .bucketNum(integer(props, "bucket.num", 16))
                .bucketQueueCapacity(integer(props, "bucket.queue.capacity", 8192))
                .ddlWaitSeconds(integer(props, "ddl.wait.seconds", 30))
                .forceSingleBucketOnUniqueIndex(bool(props, "force.single.bucket.on.unique.index", true))
                .bootstrapCollection(bool(props, "bootstrap.collection", true))
                .bootstrapIndexes(bool(props, "bootstrap.indexes", true))
                .skipTtlIndexes(bool(props, "skip.ttl.indexes", true))
                .offsetLogIntervalSeconds(integer(props, "offset.log.interval.seconds", 30))
                .fullSyncParallelism(integer(props, "full.sync.parallelism", 1))
                .fullSyncBatchSize(integer(props, "full.sync.batch.size", 1000))
                .fullSyncTaskMbSize(integer(props, "full.sync.task.mb.size", 32))
                .writeErrorHandler(new com.whaleal.third.mongo.sync.spi.SyncWriteErrorHandler() {
                    @Override
                    public void onWriteError(int bucketId, com.whaleal.third.mongo.transfer.model.TransferEvent event,
                                             Throwable error) {
                        System.err.println("[mongo-sync] write-error bucket=" + bucketId
                                + " err=" + (error == null ? "null" : error.getMessage()));
                    }
                });

        String offsetDir = props.getProperty("offset.store.dir");
        if (hasText(offsetDir)) {
            b.offsetStoreDir(offsetDir.trim());
        }
        String mongoVersion = props.getProperty("mongo.version");
        if (hasText(mongoVersion)) {
            b.mongoVersion(mongoVersion.trim());
        }
        String oplogUris = props.getProperty("source.oplog.uris");
        if (hasText(oplogUris)) {
            b.sourceOplogUrisSemicolon(oplogUris.trim());
        }
        String shardNames = props.getProperty("source.oplog.shard.names");
        if (hasText(shardNames)) {
            b.sourceOplogShardNames(shardNames.trim().split("\\s*;\\s*"));
        }
        return b;
    }

    private static MongoMultiSyncConfig.Builder buildMulti(Properties props, String sourceUri, String targetUri) {
        MongoMultiSyncConfig.Builder b = MongoMultiSyncConfig.builder()
                .sourceUri(sourceUri)
                .targetUri(targetUri)
                .namespaceWhite(props.getProperty("namespace.white"))
                .namespaceBlack(props.getProperty("namespace.black"))
                .namespaceTransform(props.getProperty("namespace.transform"))
                .captureMode(CaptureMode.valueOf(get(props, "capture.mode", "AUTO").toUpperCase()))
                .syncMode(SyncMode.valueOf(get(props, "sync.mode", "FULL_AND_INCREMENTAL").toUpperCase()))
                .fullDocument(MongoSourceConfig.FullDocumentMode.valueOf(
                        get(props, "full.document", "DEFAULT").toUpperCase()))
                .enablePreImage(bool(props, "enable.pre.image", false))
                .includeFromMigrate(bool(props, "include.from.migrate", false))
                .writeMode(WriteMode.valueOf(get(props, "write.mode", "UPSERT").toUpperCase()))
                .onConflict(OnConflict.valueOf(get(props, "on.conflict", "FAIL").toUpperCase()))
                .targetBatchSize(integer(props, "target.batch.size", 1000))
                .targetWriterThreads(integer(props, "target.writer.threads", 8))
                .bucketNum(integer(props, "bucket.num", 16))
                .bucketQueueCapacity(integer(props, "bucket.queue.capacity", 8192))
                .ddlWaitSeconds(integer(props, "ddl.wait.seconds", 30))
                .forceSingleBucketOnUniqueIndex(bool(props, "force.single.bucket.on.unique.index", true))
                .bootstrapCollection(bool(props, "bootstrap.collection", true))
                .bootstrapIndexes(bool(props, "bootstrap.indexes", true))
                .skipTtlIndexes(bool(props, "skip.ttl.indexes", true))
                .offsetLogIntervalSeconds(integer(props, "offset.log.interval.seconds", 30))
                .fullSyncParallelism(integer(props, "full.sync.parallelism", 1))
                .fullSyncBatchSize(integer(props, "full.sync.batch.size", 1000))
                .fullSyncTaskMbSize(integer(props, "full.sync.task.mb.size", 32))
                .writeErrorHandler(new com.whaleal.third.mongo.sync.spi.SyncWriteErrorHandler() {
                    @Override
                    public void onWriteError(int bucketId, com.whaleal.third.mongo.transfer.model.TransferEvent event,
                                             Throwable error) {
                        System.err.println("[mongo-sync] write-error bucket=" + bucketId
                                + " err=" + (error == null ? "null" : error.getMessage()));
                    }
                });

        String offsetDir = props.getProperty("offset.store.dir");
        if (hasText(offsetDir)) {
            b.offsetStoreDir(offsetDir.trim());
        }
        String mongoVersion = props.getProperty("mongo.version");
        if (hasText(mongoVersion)) {
            b.mongoVersion(mongoVersion.trim());
        }
        String oplogUris = props.getProperty("source.oplog.uris");
        if (hasText(oplogUris)) {
            b.sourceOplogUrisSemicolon(oplogUris.trim());
        }
        String shardNames = props.getProperty("source.oplog.shard.names");
        if (hasText(shardNames)) {
            b.sourceOplogShardNames(shardNames.trim().split("\\s*;\\s*"));
        }
        return b;
    }

    private static Properties loadArgs(String[] args) throws Exception {
        Properties props = new Properties();
        if (args == null || args.length == 0) {
            throw new IllegalArgumentException(
                    "usage: SyncMain <properties-file> | SyncMain --config <file>");
        }
        if (args.length == 1 && !args[0].startsWith("--")) {
            loadFile(props, Paths.get(args[0]));
            return props;
        }
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if ("--config".equals(a) && i + 1 < args.length) {
                loadFile(props, Paths.get(args[++i]));
            } else if ("--source-uri".equals(a) && i + 1 < args.length) {
                props.setProperty("source.uri", args[++i]);
            } else if ("--target-uri".equals(a) && i + 1 < args.length) {
                props.setProperty("target.uri", args[++i]);
            } else if ("--source-db".equals(a) && i + 1 < args.length) {
                props.setProperty("source.database", args[++i]);
            } else if ("--source-coll".equals(a) && i + 1 < args.length) {
                props.setProperty("source.collection", args[++i]);
            } else if ("--target-db".equals(a) && i + 1 < args.length) {
                props.setProperty("target.database", args[++i]);
            } else if ("--target-coll".equals(a) && i + 1 < args.length) {
                props.setProperty("target.collection", args[++i]);
            } else if ("--namespace-white".equals(a) && i + 1 < args.length) {
                props.setProperty("namespace.white", args[++i]);
            } else if ("--sync-mode".equals(a) && i + 1 < args.length) {
                props.setProperty("sync.mode", args[++i]);
            } else if ("--capture-mode".equals(a) && i + 1 < args.length) {
                props.setProperty("capture.mode", args[++i]);
            } else if ("--commit-when-ready".equals(a)) {
                props.setProperty("commit.when.ready", "true");
            } else if ("--progress-log-seconds".equals(a) && i + 1 < args.length) {
                props.setProperty("progress.log.interval.seconds", args[++i]);
            } else {
                throw new IllegalArgumentException("unknown arg: " + a);
            }
        }
        return props;
    }

    private static void loadFile(Properties props, Path path) throws Exception {
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("config file not found: " + path.toAbsolutePath());
        }
        try (InputStream in = Files.newInputStream(path)) {
            props.load(new InputStreamReader(in, StandardCharsets.UTF_8));
        }
    }

    private static boolean hasText(String s) {
        return s != null && !s.trim().isEmpty();
    }

    private static String req(Properties p, String key) {
        String v = p.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            throw new IllegalArgumentException("missing required: " + key);
        }
        return v.trim();
    }

    private static String get(Properties p, String key, String def) {
        String v = p.getProperty(key, def);
        return v == null ? def : v.trim();
    }

    private static int integer(Properties p, String key, int def) {
        String v = p.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            return def;
        }
        return Integer.parseInt(v.trim());
    }

    private static boolean bool(Properties p, String key, boolean def) {
        String v = p.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            return def;
        }
        return Boolean.parseBoolean(v.trim());
    }
}
