package com.whaleal.third.mongo.sync.offset;

import com.whaleal.third.mongo.source.spi.OplogOffsetStorage;
import com.whaleal.third.mongo.source.spi.ResumeTokenStorage;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 按源 ns 生成位点文件路径：{@code <dir>/<db>__<coll>.resume.json} / {@code .oplog.ts}。
 */
public final class FileOffsetStoreFactory {

    private final Path rootDir;

    public FileOffsetStoreFactory(String rootDir) {
        this(Paths.get(rootDir));
    }

    public FileOffsetStoreFactory(Path rootDir) {
        if (rootDir == null) {
            throw new IllegalArgumentException("offset rootDir is required");
        }
        this.rootDir = rootDir;
    }

    public Path getRootDir() {
        return rootDir;
    }

    public ResumeTokenStorage resumeTokenStorage(String sourceDatabase, String sourceCollection) {
        return new FileResumeTokenStorage(fileFor(sourceDatabase, sourceCollection, null, "resume.json"));
    }

    public OplogOffsetStorage oplogOffsetStorage(String sourceDatabase, String sourceCollection) {
        return new FileOplogOffsetStorage(fileFor(sourceDatabase, sourceCollection, null, "oplog.ts"));
    }

    /** 分片 OPLOG：按 shard 名隔离位点文件。 */
    public OplogOffsetStorage oplogOffsetStorage(String sourceDatabase,
                                                 String sourceCollection,
                                                 String shardName) {
        return new FileOplogOffsetStorage(fileFor(sourceDatabase, sourceCollection, shardName, "oplog.ts"));
    }

    private Path fileFor(String database, String collection, String shardName, String suffix) {
        String safeDb = sanitize(database);
        String safeColl = sanitize(collection);
        String base = safeDb + "__" + safeColl;
        if (shardName != null && !shardName.isEmpty()) {
            base = base + "__" + sanitize(shardName);
        }
        return rootDir.resolve(base + "." + suffix);
    }

    private static String sanitize(String name) {
        if (name == null || name.isEmpty()) {
            return "_";
        }
        return name.replaceAll("[^a-zA-Z0-9._-]", "_");
    }
}
