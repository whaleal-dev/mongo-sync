package com.whaleal.third.mongo.sync.spi;

import com.whaleal.third.mongo.transfer.model.TransferEvent;

/**
 * 分桶写入失败回调，避免静默丢事件。
 */
public interface SyncWriteErrorHandler {

    /**
     * @param bucketId 分桶号
     * @param event    失败事件（可能为 null）
     * @param error    异常
     */
    void onWriteError(int bucketId, TransferEvent event, Throwable error);
}
