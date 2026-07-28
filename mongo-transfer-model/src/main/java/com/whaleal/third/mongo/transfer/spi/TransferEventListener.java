package com.whaleal.third.mongo.transfer.spi;

import com.whaleal.third.mongo.transfer.model.TransferEvent;

/**
 * 文档变更事件回调（CRUD / snapshot）。
 */
public interface TransferEventListener {

    void onEvent(TransferEvent event);
}
