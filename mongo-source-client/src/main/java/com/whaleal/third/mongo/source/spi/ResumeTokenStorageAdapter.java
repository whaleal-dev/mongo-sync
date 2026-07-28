package com.whaleal.third.mongo.source.spi;

import com.whaleal.third.mongo.source.model.ResumeToken;

/**
 * {@link ResumeTokenStorage} 适配器，默认空实现。
 */
public abstract class ResumeTokenStorageAdapter implements ResumeTokenStorage {

    @Override
    public ResumeToken load() {
        return ResumeToken.empty();
    }

    @Override
    public void save(ResumeToken token) {
    }
}
