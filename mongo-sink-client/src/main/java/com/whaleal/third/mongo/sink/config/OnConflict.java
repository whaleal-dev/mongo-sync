package com.whaleal.third.mongo.sink.config;

/**
 * 唯一键 / 主键冲突（Mongo duplicate key，error 11000）处理策略。
 * <p>
 * 与 {@link WriteMode} 配合：
 * <ul>
 *   <li>{@link WriteMode#UPSERT}：insert 本身已按 _id upsert，一般不触发主键冲突</li>
 *   <li>{@link WriteMode#STRICT}：insert 使用 InsertOne，冲突时由本策略决定</li>
 * </ul>
 * 唯一二级索引冲突时，即使 UPSERT 也可能报错；此时 {@link #SKIP} 可跳过并记日志。
 */
public enum OnConflict {

    /**
     * 冲突则失败（抛 {@link com.whaleal.third.mongo.sink.exception.SinkWriteException}）。
     */
    FAIL,

    /**
     * 跳过冲突文档，记录日志，继续后续写入。
     */
    SKIP,

    /**
     * 将冲突的 insert 转为按 _id 的 ReplaceOne upsert 后重试；
     * 非 insert 或无法转换的冲突则跳过并记日志。
     */
    UPSERT
}
