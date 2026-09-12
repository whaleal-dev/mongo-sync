package com.whaleal.third.mongo.source.config;

/**
 * 同步模式。
 * <ul>
 *   <li>{@link #FULL} — 仅全量，结束后停止</li>
 *   <li>{@link #FULL_AND_INCREMENTAL} — 全量与增量<strong>并行</strong>，全量结束后持续增量</li>
 *   <li>{@link #FULL_THEN_CATCH_UP} — 先全量，再追增量窗口上界，追平后停止（<strong>串行</strong>）</li>
 *   <li>{@link #INCREMENTAL} — 仅增量</li>
 * </ul>
 * <p>
 * {@code AND} 表示全量与增量并行并持续；{@code THEN} 表示先全量、再追平、再停。
 */
public enum SyncMode {

    FULL,
    FULL_AND_INCREMENTAL,
    FULL_THEN_CATCH_UP,
    INCREMENTAL;

    /** 是否包含全量快照。 */
    public boolean includesFull() {
        return this == FULL || this == FULL_AND_INCREMENTAL || this == FULL_THEN_CATCH_UP;
    }

    /** 是否包含增量消费。 */
    public boolean includesIncremental() {
        return this != FULL;
    }

    /** 增量是否在追平窗口后停止。 */
    public boolean catchUpThenStop() {
        return this == FULL_THEN_CATCH_UP;
    }

    /** 全量与增量是否并行执行。仅 {@link #FULL_AND_INCREMENTAL}。 */
    public boolean parallelFullAndIncremental() {
        return this == FULL_AND_INCREMENTAL;
    }
}
