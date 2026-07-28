package com.whaleal.third.mongo.source.spi;

import com.whaleal.third.mongo.source.model.ResumeToken;

/**
 * ChangeStream 位点存储（ResumeToken）。
 * <p>
 * Oplog 请实现 {@link OplogOffsetStorage}。
 */
public interface ResumeTokenStorage {

    ResumeToken load();

    void save(ResumeToken token);
}
