package com.whaleal.third.mongo.source.oplog;

/**
 * MongoDB 服务端版本。OPLOG 模式下必须由客户端显式传入，不做自动探测。
 */
public final class MongoVersion {

    private final int major;
    private final int minor;
    private final int patch;
    private final String raw;

    private MongoVersion(int major, int minor, int patch, String raw) {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
        this.raw = raw;
    }

    public static MongoVersion parse(String version) {
        if (version == null || version.trim().isEmpty()) {
            throw new IllegalArgumentException("mongoVersion is required");
        }
        String raw = version.trim();
        String numeric = raw.split("-")[0].trim();
        String[] parts = numeric.split("\\.");
        if (parts.length < 1) {
            throw new IllegalArgumentException("Invalid mongoVersion: " + version);
        }
        try {
            int major = Integer.parseInt(parts[0]);
            int minor = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
            int patch = parts.length > 2 ? Integer.parseInt(parts[2].replaceAll("[^0-9].*$", "")) : 0;
            return new MongoVersion(major, minor, patch, raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid mongoVersion: " + version, e);
        }
    }

    public int getMajor() {
        return major;
    }

    public int getMinor() {
        return minor;
    }

    public int getPatch() {
        return patch;
    }

    public String getRaw() {
        return raw;
    }

    /**
     * MongoDB 7.0 及以后禁止 OPLOG，必须使用 ChangeStream。
     */
    public boolean supportsOplog() {
        return major < 7;
    }

    /**
     * ChangeStream 自 MongoDB 3.6 起支持（副本集与分片集群）。
     */
    public boolean supportsChangeStream() {
        return isAtLeast(3, 6);
    }

    public OplogFormatVersion toOplogFormat() {
        if (!supportsOplog()) {
            throw new IllegalStateException(
                    "MongoDB " + raw + " does not support OPLOG capture; use CHANGE_STREAM instead");
        }
        // V1: 3.2 / 3.4
        if (major < 3 || (major == 3 && minor < 6)) {
            return OplogFormatVersion.V1;
        }
        // V2: 3.6 / 4.x
        if (major < 5) {
            return OplogFormatVersion.V2;
        }
        // V3: 5.0 / 6.0
        return OplogFormatVersion.V3;
    }

    public boolean isAtLeast(int major, int minor) {
        return this.major > major || (this.major == major && this.minor >= minor);
    }

    @Override
    public String toString() {
        return raw;
    }
}
