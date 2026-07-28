package com.whaleal.third.mongo.sync.error;

/**
 * 带错误码的运行异常。
 */
public class MongoSyncException extends RuntimeException {

    private final MongoSyncErrorCode errorCode;

    public MongoSyncException(MongoSyncErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public MongoSyncException(MongoSyncErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public MongoSyncErrorCode getErrorCode() {
        return errorCode;
    }

    public String getCode() {
        return errorCode == null ? "MSYNC_UNKNOWN" : errorCode.code();
    }
}
