package com.whaleal.third.mongo.source.spi;

import com.whaleal.third.mongo.source.model.OplogOffset;

/**
 * {@link OplogOffsetStorage} 适配器，默认空实现。
 */
public abstract class OplogOffsetStorageAdapter implements OplogOffsetStorage {

    @Override
    public OplogOffset load() {
        return OplogOffset.empty();
    }

    @Override
    public void save(OplogOffset offset) {
    }
}
