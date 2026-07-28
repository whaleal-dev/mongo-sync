package com.whaleal.third.mongo.sync.sdk;

/**
 * 迁移状态（对齐 mongosync 的 start/progress/commit 生命周期）。
 */
public enum MigrationState {
    IDLE,
    RUNNING,
    CAN_COMMIT,
    COMMITTING,
    COMMITTED,
    STOPPED,
    ERROR
}
