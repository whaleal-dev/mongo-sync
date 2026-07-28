package com.whaleal.third.mongo.source.topology;

/**
 * 源端 MongoDB 架构（由 {@code hello}/{@code isMaster} 探测）。
 */
public enum SourceTopology {

    /** 单节点（无 replica set / 非 mongos）。 */
    STANDALONE,

    /** 副本集。 */
    REPLICA_SET,

    /** 分片集群（连接落在 mongos，{@code msg=isdbgrid}）。 */
    SHARDING
}
