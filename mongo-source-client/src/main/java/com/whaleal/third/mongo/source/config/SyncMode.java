package com.whaleal.third.mongo.source.config;

/**
 * 同步模式。
 * <ul>
 *   <li>{@link #FULL} — 仅全量，结束后停止</li>
 *   <li>{@link #FULL_AND_INCREMENTAL} — 全量与增量<strong>并行</strong>，全量结束后持续增量</li>
 *   <li>{@link #FULL_AND_CATCH_UP} — 全量与增量<strong>并行</strong>，全量结束后追平窗口上界再停止</li>
 *   <li>{@link #INCREMENTAL} — 仅增量</li>
 * </ul>
 * <p>
 * 命名用 {@code AND} 强调并行，而非串行的 THEN。
 */
public enum SyncMode {

    FULL,
    FULL_AND_INCREMENTAL,
    FULL_AND_CATCH_UP,
    INCREMENTAL;

    /** 是否包含全量快照。 */
    public boolean includesFull() {
        return this == FULL || this == FULL_AND_INCREMENTAL || this == FULL_AND_CATCH_UP;
    }

    /** 是否包含增量消费。 */
    public boolean includesIncremental() {
        return this != FULL;
    }

    /** 增量是否在追平窗口后停止。 */
    public boolean catchUpThenStop() {
        return this == FULL_AND_CATCH_UP;
    }

    /** 全量与增量是否并行执行。 */
    public boolean parallelFullAndIncremental() {
        return this == FULL_AND_INCREMENTAL || this == FULL_AND_CATCH_UP;
    }
}
