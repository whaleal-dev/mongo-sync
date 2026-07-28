package com.whaleal.third.mongo.sync.pipeline;

import com.whaleal.third.mongo.transfer.model.TransferEvent;

/**
 * Disruptor RingBuffer 槽位（可变对象，避免每次分配）。
 */
public final class TransferEventSlot {

    private TransferEvent event;

    public TransferEvent getEvent() {
        return event;
    }

    public void setEvent(TransferEvent event) {
        this.event = event;
    }

    public void clear() {
        this.event = null;
    }
}
