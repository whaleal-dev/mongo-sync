package com.whaleal.third.mongo.source.spi;

import com.whaleal.third.mongo.source.model.OplogOffset;

/**
 * Oplog 位点存储（BsonTimestamp / {@link OplogOffset}）。
 * <p>
 * ChangeStream 请实现 {@link ResumeTokenStorage}。
 */
public interface OplogOffsetStorage {

    OplogOffset load();

    void save(OplogOffset offset);
}
