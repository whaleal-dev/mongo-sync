package com.whaleal.third.mongo.source.exception;

public class SourceConnectException extends SourceException {

    public SourceConnectException(String message) {
        super(message);
    }

    public SourceConnectException(String message, Throwable cause) {
        super(message, cause);
    }
}
