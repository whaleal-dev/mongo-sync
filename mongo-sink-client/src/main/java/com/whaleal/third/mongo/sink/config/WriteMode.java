package com.whaleal.third.mongo.sink.config;

/**
 * 写入语义。
 * <p>
 * 主键冲突的细粒度策略见 {@link OnConflict}。
 */
public enum WriteMode {

    /**
     * insert / replace / update / delete 按 op 严格执行；
     * insert 默认 InsertOne，冲突行为由 {@link OnConflict} 决定。
     */
    STRICT,

    /**
     * 对 insert/update/replace 使用 upsert（按 _id 或自定义 filter），delete 仍为删除。
     * 适合 CDC 幂等落地（允许重复投递）。
     */
    UPSERT
}
