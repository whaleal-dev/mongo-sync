package com.whaleal.third.mongo.sync.offset;

import com.whaleal.third.mongo.source.model.OplogOffset;
import com.whaleal.third.mongo.source.spi.OplogOffsetStorage;

import java.util.concurrent.atomic.AtomicReference;

/** 内存 OplogOffset（进程内重启丢失）。 */
public final class MemoryOplogOffsetStorage implements OplogOffsetStorage {

    private final AtomicReference<OplogOffset> ref = new AtomicReference<OplogOffset>();

    @Override
    public OplogOffset load() {
        return ref.get();
    }

    @Override
    public void save(OplogOffset offset) {
        ref.set(offset);
    }
}
