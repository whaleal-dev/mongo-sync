package com.whaleal.third.mongo.source.sdk;

public interface SourceListener {

    void start();

    /** 暂停全量+增量（停协调线程与增量线程）。 */
    void pause();

    /**
     * 仅暂停增量捕获；全量扫描可继续（对齐 photon {@code REAL_TIME_SLEEP}）。
     * 已暂停则幂等。
     */
    void pauseIncremental();

    /** 恢复增量捕获；未暂停则幂等。 */
    void resumeIncremental();

    /** 增量是否处于独立暂停。 */
    boolean isIncrementalPaused();

    /**
     * 捕获窗口余量（秒）：锚定位点 − oplog 最早条目。
     * 无法探测时返回 {@code null}。
     */
    Long getWindowRemainingSeconds();

    void stop();
}
