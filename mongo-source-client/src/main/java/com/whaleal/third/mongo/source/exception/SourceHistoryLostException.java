package com.whaleal.third.mongo.source.exception;

public class SourceHistoryLostException extends SourceException {

    public SourceHistoryLostException(String message) {
        super(message);
    }

    public SourceHistoryLostException(String message, Throwable cause) {
        super(message, cause);
    }
}
