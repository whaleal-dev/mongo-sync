package com.whaleal.third.mongo.transfer.spi;

import com.whaleal.third.mongo.transfer.model.DdlEvent;

/**
 * DDL / 索引事件回调。未配置时捕获端仍可推进位点。
 */
public interface DdlEventListener {

    void onDdl(DdlEvent event);
}
