package com.whaleal.third.mongo.transfer.spi;

import com.whaleal.third.mongo.transfer.model.DdlEvent;
import com.whaleal.third.mongo.transfer.model.TransferEvent;

/**
 * 目标端写入 SPI：只识别 {@link TransferEvent} / {@link DdlEvent}，不关心上游捕获协议。
 * <p>
 * MongoDB 与 Kafka 等目标形态各自实现本接口，Sync 编排层只依赖本契约。
 */
public interface TransferSink extends AutoCloseable {

    /**
     * 写入一条文档事件。
     *
     * @return 本次写入序号；{@code 0} 表示未产生写入
     */
    long write(TransferEvent event);

    /**
     * 已确认落地的最大写入序号：所有 {@code seq <= landedThrough()} 的写入都已在目标端生效。
     */
    long landedThrough();

    /** 先排空在途 CRUD，再落地 DDL。 */
    void applyDdl(DdlEvent event);

    /** 运行中按唯一索引策略调整有序写入（Mongo bulk ordered；Kafka 可忽略）。 */
    void setOrdered(boolean ordered);

    /** 刷写缓冲并等待在途完成（不关闭资源）。 */
    void flushAndWait();

    @Override
    void close();
}
