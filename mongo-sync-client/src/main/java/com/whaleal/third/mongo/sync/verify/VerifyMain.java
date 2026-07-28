package com.whaleal.third.mongo.sync.verify;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.whaleal.third.mongo.sync.ns.CollectionDiscovery;
import com.whaleal.third.mongo.sync.ns.NamespaceFilter;
import com.whaleal.third.mongo.sync.ns.NamespaceMapper;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 数据比对入口。
 * <p>
 * 用法：
 * <pre>
 *   java -cp mongo-sync-client-...jar:... \
 *     com.whaleal.third.mongo.sync.verify.VerifyMain /path/to/mongo-verify.properties
 *
 *   # 或简参（单表 FULL）
 *   java ... VerifyMain \
 *     --source-uri mongodb://src --target-uri mongodb://tgt \
 *     --source-db demo --source-coll orders
 * </pre>
 * <p>
 * 退出码：0 全部通过；1 存在差异；2 参数/运行错误。
 */
public final class VerifyMain {

    private VerifyMain() {
    }

    public static void main(String[] args) {
        int code = 2;
        try {
            code = run(args);
        } catch (Exception e) {
            System.err.println("[mongo-verify] ERROR " + e.getMessage());
            e.printStackTrace(System.err);
            code = 2;
        }
        System.exit(code);
    }

    static int run(String[] args) throws Exception {
        Properties props = loadArgs(args);
        String sourceUri = req(props, "source.uri");
        String targetUri = req(props, "target.uri");
        VerifyMode mode = VerifyMode.valueOf(get(props, "verify.mode", "FULL").toUpperCase());
        int maxSamples = integer(props, "verify.max.samples", 50);
        int batchSize = integer(props, "verify.batch.size", 500);
        java.util.Set<String> ignore = DataVerifier.parseIgnoreFields(get(props, "verify.ignore.fields", ""));

        MongoClient source = MongoClients.create(sourceUri);
        MongoClient target = MongoClients.create(targetUri);
        try {
            DataVerifier verifier = new DataVerifier(source, target, mode, ignore, maxSamples, batchSize);
            List<NamespaceMapper.NsPair> pairs = resolvePairs(props, source);
            if (pairs.isEmpty()) {
                throw new IllegalStateException("no collections to verify");
            }

            System.err.println("[mongo-verify] mode=" + mode + " collections=" + pairs.size());
            boolean allPass = true;
            List<CollectionVerifyReport> reports = new ArrayList<CollectionVerifyReport>();
            for (NamespaceMapper.NsPair pair : pairs) {
                CollectionVerifyReport report = verifier.verify(
                        pair.sourceDatabase, pair.sourceCollection,
                        pair.targetDatabase, pair.targetCollection);
                reports.add(report);
                System.out.println(report);
                for (String sample : report.getSamples()) {
                    System.out.println("  - " + sample);
                }
                if (!report.isPassed()) {
                    allPass = false;
                }
            }

            int fail = 0;
            for (CollectionVerifyReport r : reports) {
                if (!r.isPassed()) {
                    fail++;
                }
            }
            System.err.println("[mongo-verify] done total=" + reports.size()
                    + " pass=" + (reports.size() - fail) + " fail=" + fail);
            return allPass ? 0 : 1;
        } finally {
            source.close();
            target.close();
        }
    }

    private static List<NamespaceMapper.NsPair> resolvePairs(Properties props, MongoClient source) {
        String white = props.getProperty("namespace.white");
        String black = props.getProperty("namespace.black");
        String transform = props.getProperty("namespace.transform");
        if ((white != null && !white.trim().isEmpty()) || (black != null && !black.trim().isEmpty())) {
            NamespaceFilter filter = NamespaceFilter.of(white, black);
            NamespaceMapper mapper = NamespaceMapper.of(transform);
            return CollectionDiscovery.discover(source, filter, mapper);
        }

        String sdb = req(props, "source.database");
        String scoll = req(props, "source.collection");
        String tdb = get(props, "target.database", sdb);
        String tcoll = get(props, "target.collection", scoll);
        if (transform != null && !transform.trim().isEmpty()) {
            NamespaceMapper.NsPair mapped = NamespaceMapper.of(transform).map(sdb, scoll);
            List<NamespaceMapper.NsPair> one = new ArrayList<NamespaceMapper.NsPair>();
            one.add(mapped);
            return one;
        }
        List<NamespaceMapper.NsPair> one = new ArrayList<NamespaceMapper.NsPair>();
        one.add(new NamespaceMapper.NsPair(sdb, scoll, tdb, tcoll));
        return one;
    }

    private static Properties loadArgs(String[] args) throws Exception {
        Properties props = new Properties();
        if (args == null || args.length == 0) {
            throw new IllegalArgumentException(
                    "usage: VerifyMain <properties-file> | VerifyMain --config <file> | VerifyMain --source-uri ...");
        }
        if (args.length == 1 && !args[0].startsWith("--")) {
            Path path = Paths.get(args[0]);
            try (InputStream in = Files.newInputStream(path)) {
                props.load(new InputStreamReader(in, StandardCharsets.UTF_8));
            }
            return props;
        }
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if ("--source-uri".equals(a) && i + 1 < args.length) {
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
            } else if ("--mode".equals(a) && i + 1 < args.length) {
                props.setProperty("verify.mode", args[++i]);
            } else if ("--namespace-white".equals(a) && i + 1 < args.length) {
                props.setProperty("namespace.white", args[++i]);
            } else if ("--config".equals(a) && i + 1 < args.length) {
                Path path = Paths.get(args[++i]);
                try (InputStream in = Files.newInputStream(path)) {
                    props.load(new InputStreamReader(in, StandardCharsets.UTF_8));
                }
            } else {
                throw new IllegalArgumentException("unknown arg: " + a);
            }
        }
        return props;
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
}
