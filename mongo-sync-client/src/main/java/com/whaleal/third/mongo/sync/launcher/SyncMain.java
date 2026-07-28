package com.whaleal.third.mongo.sync.launcher;

import com.whaleal.third.mongo.sink.config.OnConflict;
import com.whaleal.third.mongo.sink.config.WriteMode;
import com.whaleal.third.mongo.source.config.CaptureMode;
import com.whaleal.third.mongo.source.config.MongoSourceConfig;
import com.whaleal.third.mongo.source.config.SyncMode;
import com.whaleal.third.mongo.sync.config.MongoMultiSyncConfig;
import com.whaleal.third.mongo.sync.config.MongoSyncConfig;
import com.whaleal.third.mongo.sync.error.MongoSyncErrorCode;
import com.whaleal.third.mongo.sync.error.MongoSyncException;
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
        } catch (MongoSyncException e) {
            System.err.println("[mongo-sync] ERROR code=" + e.getCode() + " message=" + e.getMessage());
            code = 2;
        } catch (Exception e) {
            System.err.println("[mongo-sync] ERROR " + e.getMessage());
            e.printStackTrace(System.err);
            code = 2;
        }
        System.exit(code);
    }

    static int run(String[] args) throws Exception {
        Properties props = loadArgs(args);
        validateStartupProps(props);
        String sourceUri = req(props, "source.uri");
        String targetUri = req(props, "target.uri");

        boolean multi = hasText(props.getProperty("namespace.white"))
                || hasText(props.getProperty("namespace.black"));
        final boolean autoCommitWhenReady = bool(props, "commit.when.ready", false);
        final int progressLogSeconds = integer(props, "progress.log.interval.seconds", 10);
        final boolean httpEnabled = bool(props, "http.enabled", false);
        final String httpHost = get(props, "http.host", "127.0.0.1");
        final int httpPort = integer(props, "http.port", 27182);

        final AtomicReference<AutoCloseable> clientRef = new AtomicReference<AutoCloseable>();
        final AtomicReference<SyncHttpServer> httpServerRef = new AtomicReference<SyncHttpServer>();
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
                SyncHttpServer http = httpServerRef.get();
                if (http != null) {
                    try {
                        http.close();
                    } catch (Exception e) {
                        System.err.println("[mongo-sync] http close error: " + e.getMessage());
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
            if (httpEnabled) {
                httpServerRef.set(SyncHttpServer.start(
                        httpHost,
                        httpPort,
                        () -> multiClient.progress(),
                        () -> multiClient.pause(),
                        () -> {
                            multiClient.resume();
                            return multiClient.progress();
                        },
                        () -> multiClient.commit()));
                System.err.println("[mongo-sync] http control listening at http://" + httpHost + ":" + httpPort);
            }
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
            if (httpEnabled) {
                httpServerRef.set(SyncHttpServer.start(
                        httpHost,
                        httpPort,
                        () -> sync.progress(),
                        () -> sync.pause(),
                        () -> {
                            sync.resume();
                            return sync.progress();
                        },
                        () -> sync.commit()));
                System.err.println("[mongo-sync] http control listening at http://" + httpHost + ":" + httpPort);
            }
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
                    System.err.println("[mongo-sync] progress " + formatProgress(p));
                    if (autoCommitWhenReady && p.isCanCommit()) {
                        MigrationProgress committed = sync.commit();
                        System.err.println("[mongo-sync] committed " + formatProgress(committed));
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
                    System.err.println("[mongo-sync] progress " + formatProgress(p));
                    if (autoCommitWhenReady && p.isCanCommit()) {
                        MigrationProgress committed = sync.commit();
                        System.err.println("[mongo-sync] committed " + formatProgress(committed));
                        stop.countDown();
                    }
                } catch (Exception e) {
                    System.err.println("[mongo-sync] progress/commit error: " + e.getMessage());
                }
            }
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    private static String formatProgress(MigrationProgress p) {
        StringBuilder sb = new StringBuilder();
        appendKv(sb, "ns", p.getNamespace());
        appendKv(sb, "phase", p.getPhase());
        appendKv(sb, "state", String.valueOf(p.getState()));
        appendKv(sb, "mode", p.getSyncMode());
        appendKv(sb, "capture", p.getCaptureMode());
        appendKv(sb, "topology", p.getTopology());
        appendKv(sb, "fullCopied", String.valueOf(p.getCopiedDocuments()));
        if (p.getEstimatedTotalDocuments() > 0) {
            appendKv(sb, "fullEstimated", String.valueOf(p.getEstimatedTotalDocuments()));
            appendKv(sb, "fullPercent", String.valueOf(p.getFullSyncPercent()));
            appendKv(sb, "fullRemaining", String.valueOf(p.getRemainingDocumentsEstimate()));
        }
        appendKv(sb, "incrEvents", String.valueOf(p.getIncrementalEvents()));
        appendKv(sb, "ddlEvents", String.valueOf(p.getDdlEvents()));
        appendKv(sb, "inflight", String.valueOf(p.getInflightEvents()));
        if (p.getLagMs() != null) {
            appendKv(sb, "lagMs", String.valueOf(p.getLagMs()));
        }
        appendKv(sb, "canCommit", String.valueOf(p.isCanCommit()));
        appendKv(sb, "readiness", p.getCommitReadiness());
        if (p.getElapsedMs() > 0) {
            appendKv(sb, "elapsedMs", String.valueOf(p.getElapsedMs()));
        }
        if (p.getNamespaceCount() > 1) {
            appendKv(sb, "namespaces", String.valueOf(p.getNamespaceCount()));
        }
        if (p.getShardSourceCount() > 0) {
            appendKv(sb, "shardSources", String.valueOf(p.getShardSourceCount()));
        }
        if (p.getDetail() != null && !p.getDetail().isEmpty()) {
            appendKv(sb, "detail", p.getDetail());
        }
        return sb.toString();
    }

    private static void appendKv(StringBuilder sb, String key, String value) {
        if (key == null || key.isEmpty() || value == null || value.isEmpty()) {
            return;
        }
        if (sb.length() > 0) {
            sb.append(' ');
        }
        sb.append(key).append('=').append(normalizeLogValue(value));
    }

    private static String normalizeLogValue(String value) {
        String v = value.trim();
        if (v.isEmpty()) {
            return "\"\"";
        }
        boolean simple = true;
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            if (!(Character.isLetterOrDigit(c) || c == '_' || c == '-' || c == '.' || c == ':' || c == '/' || c == '(' || c == ')')) {
                simple = false;
                break;
            }
        }
        if (simple) {
            return v;
        }
        return "\"" + v.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
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
                .captureMode(parseCaptureMode(props))
                .syncMode(parseSyncMode(props))
                .fullDocument(parseFullDocument(props))
                .enablePreImage(bool(props, "enable.pre.image", false))
                .includeFromMigrate(bool(props, "include.from.migrate", false))
                .writeMode(parseWriteMode(props))
                .onConflict(parseOnConflict(props))
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
                .captureMode(parseCaptureMode(props))
                .syncMode(parseSyncMode(props))
                .fullDocument(parseFullDocument(props))
                .enablePreImage(bool(props, "enable.pre.image", false))
                .includeFromMigrate(bool(props, "include.from.migrate", false))
                .writeMode(parseWriteMode(props))
                .onConflict(parseOnConflict(props))
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
            throw new MongoSyncException(MongoSyncErrorCode.CONFIG_REQUIRED,
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
                throw new MongoSyncException(MongoSyncErrorCode.ARGUMENT_UNKNOWN, "unknown arg: " + a);
            }
        }
        return props;
    }

    private static void loadFile(Properties props, Path path) throws Exception {
        if (!Files.isRegularFile(path)) {
            throw new MongoSyncException(MongoSyncErrorCode.FILE_NOT_FOUND,
                    "config file not found: " + path.toAbsolutePath());
        }
        try (InputStream in = Files.newInputStream(path)) {
            props.load(new InputStreamReader(in, StandardCharsets.UTF_8));
        }
    }

    private static void validateStartupProps(Properties props) {
        req(props, "source.uri");
        req(props, "target.uri");

        boolean multi = hasText(props.getProperty("namespace.white"))
                || hasText(props.getProperty("namespace.black"));
        if (multi) {
            if (hasText(props.getProperty("source.database")) || hasText(props.getProperty("source.collection"))) {
                throw new MongoSyncException(MongoSyncErrorCode.CONFIG_INVALID,
                        "multi-sync uses namespace.white/namespace.black; do not mix source.database/source.collection");
            }
        } else {
            req(props, "source.database");
            req(props, "source.collection");
        }

        parseCaptureMode(props);
        parseSyncMode(props);
        parseFullDocument(props);
        parseWriteMode(props);
        parseOnConflict(props);

        int progressLogSeconds = integer(props, "progress.log.interval.seconds", 10);
        if (progressLogSeconds < 0) {
            throw new MongoSyncException(MongoSyncErrorCode.CONFIG_INVALID,
                    "progress.log.interval.seconds must be >= 0");
        }
        int batchSize = integer(props, "target.batch.size", 1000);
        int writerThreads = integer(props, "target.writer.threads", 8);
        int bucketNum = integer(props, "bucket.num", 16);
        if (batchSize <= 0 || writerThreads <= 0 || bucketNum <= 0) {
            throw new MongoSyncException(MongoSyncErrorCode.CONFIG_INVALID,
                    "target.batch.size, target.writer.threads, bucket.num must be > 0");
        }
    }

    private static CaptureMode parseCaptureMode(Properties props) {
        try {
            return CaptureMode.valueOf(get(props, "capture.mode", "AUTO").toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new MongoSyncException(MongoSyncErrorCode.CONFIG_INVALID,
                    "invalid capture.mode, expected AUTO|CHANGE_STREAM|OPLOG", e);
        }
    }

    private static SyncMode parseSyncMode(Properties props) {
        try {
            return SyncMode.valueOf(get(props, "sync.mode", "FULL_AND_INCREMENTAL").toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new MongoSyncException(MongoSyncErrorCode.CONFIG_INVALID,
                    "invalid sync.mode, expected FULL|FULL_AND_INCREMENTAL|FULL_AND_CATCH_UP|INCREMENTAL", e);
        }
    }

    private static MongoSourceConfig.FullDocumentMode parseFullDocument(Properties props) {
        try {
            return MongoSourceConfig.FullDocumentMode.valueOf(get(props, "full.document", "DEFAULT").toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new MongoSyncException(MongoSyncErrorCode.CONFIG_INVALID,
                    "invalid full.document value", e);
        }
    }

    private static WriteMode parseWriteMode(Properties props) {
        try {
            return WriteMode.valueOf(get(props, "write.mode", "UPSERT").toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new MongoSyncException(MongoSyncErrorCode.CONFIG_INVALID,
                    "invalid write.mode, expected STRICT|UPSERT", e);
        }
    }

    private static OnConflict parseOnConflict(Properties props) {
        try {
            return OnConflict.valueOf(get(props, "on.conflict", "FAIL").toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new MongoSyncException(MongoSyncErrorCode.CONFIG_INVALID,
                    "invalid on.conflict, expected FAIL|SKIP|UPSERT", e);
        }
    }

    private static boolean hasText(String s) {
        return s != null && !s.trim().isEmpty();
    }

    private static String req(Properties p, String key) {
        String v = p.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            throw new MongoSyncException(MongoSyncErrorCode.CONFIG_REQUIRED, "missing required: " + key);
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
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            throw new MongoSyncException(MongoSyncErrorCode.CONFIG_INVALID,
                    "invalid integer for " + key + ": " + v, e);
        }
    }

    private static boolean bool(Properties p, String key, boolean def) {
        String v = p.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            return def;
        }
        return Boolean.parseBoolean(v.trim());
    }
}
