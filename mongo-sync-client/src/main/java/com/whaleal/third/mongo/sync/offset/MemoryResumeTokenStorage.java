package com.whaleal.third.mongo.sync.offset;

import com.whaleal.third.mongo.source.model.ResumeToken;
import com.whaleal.third.mongo.source.spi.ResumeTokenStorage;

import java.util.concurrent.atomic.AtomicReference;

/** 内存 ResumeToken（进程内重启丢失）。 */
public final class MemoryResumeTokenStorage implements ResumeTokenStorage {

    private final AtomicReference<ResumeToken> ref = new AtomicReference<ResumeToken>(ResumeToken.empty());

    @Override
    public ResumeToken load() {
        ResumeToken token = ref.get();
        return token == null ? ResumeToken.empty() : token;
    }

    @Override
    public void save(ResumeToken token) {
        ref.set(token == null ? ResumeToken.empty() : token);
    }
}
