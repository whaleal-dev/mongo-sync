package com.whaleal.third.mongo.sync.error;

/**
 * 面向启动校验与运行控制的错误码。
 */
public enum MongoSyncErrorCode {

    CONFIG_REQUIRED("MSYNC_CFG_001"),
    CONFIG_INVALID("MSYNC_CFG_002"),
    ARGUMENT_UNKNOWN("MSYNC_ARG_001"),
    FILE_NOT_FOUND("MSYNC_FILE_001"),
    CLIENT_STATE_INVALID("MSYNC_STATE_001"),
    PAUSE_NOT_ALLOWED("MSYNC_CTL_001"),
    RESUME_NOT_ALLOWED("MSYNC_CTL_002"),
    COMMIT_NOT_ALLOWED("MSYNC_CTL_003"),
    /** 多表 pause/commit 部分子任务失败（已尽力处理全部子任务）。 */
    MULTI_OPERATION_PARTIAL_FAILURE("MSYNC_CTL_004");

    private final String code;

    MongoSyncErrorCode(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }
}
