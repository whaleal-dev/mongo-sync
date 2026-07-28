package com.whaleal.third.mongo.source.topology;

/**
 * 分片集群中一个 shard 的连接信息（用于 OPLOG 多源读）。
 */
public final class ShardEndpoint {

    private final String shardId;
    private final String uri;
    private final String replicaSet;

    public ShardEndpoint(String shardId, String uri, String replicaSet) {
        this.shardId = shardId;
        this.uri = uri;
        this.replicaSet = replicaSet;
    }

    public String getShardId() {
        return shardId;
    }

    public String getUri() {
        return uri;
    }

    public String getReplicaSet() {
        return replicaSet;
    }

    @Override
    public String toString() {
        return shardId + "=" + uri;
    }
}
