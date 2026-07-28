package com.whaleal.third.mongo.transfer.model;

/**
 * 事件来源元信息（库 / 集合 / 集群时间），与捕获协议无关。
 */
public class TransferSource {

    private String db;
    private String collection;
    private Long clusterTime;

    public TransferSource() {
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public Long getClusterTime() {
        return clusterTime;
    }

    public void setClusterTime(Long clusterTime) {
        this.clusterTime = clusterTime;
    }

    public static class Builder {
        private String db;
        private String collection;
        private Long clusterTime;

        public Builder db(String db) {
            this.db = db;
            return this;
        }

        public Builder collection(String collection) {
            this.collection = collection;
            return this;
        }

        public Builder clusterTime(Long clusterTime) {
            this.clusterTime = clusterTime;
            return this;
        }

        public TransferSource build() {
            TransferSource source = new TransferSource();
            source.db = this.db;
            source.collection = this.collection;
            source.clusterTime = this.clusterTime;
            return source;
        }
    }
}
