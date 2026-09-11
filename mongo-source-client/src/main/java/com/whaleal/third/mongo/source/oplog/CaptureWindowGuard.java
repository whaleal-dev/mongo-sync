package com.whaleal.third.mongo.source.oplog;

import org.bson.BsonTimestamp;

/**
 * 捕获窗口余量计算（学 DataPai：锚定位点相对 oplog 最早条目的秒差）。
 * <p>
 * 全量∥增量期间，若 earliest 逼近锚定位点，重连/回放可能 {@code CappedPositionLost} / history lost。
 */
public final class CaptureWindowGuard {

    private CaptureWindowGuard() {
    }

    /**
     * @return {@code anchor.time - earliest.time}（秒）；任一为空则 null
     */
    public static Long remainingSeconds(BsonTimestamp anchor, BsonTimestamp earliest) {
        if (anchor == null || earliest == null) {
            return null;
        }
        return (long) anchor.getTime() - (long) earliest.getTime();
    }

    public static boolean shouldWarn(Long remainingSeconds, int warnThresholdSeconds) {
        return warnThresholdSeconds > 0
                && remainingSeconds != null
                && remainingSeconds.longValue() <= warnThresholdSeconds;
    }
}
