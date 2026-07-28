package com.whaleal.third.mongo.sink.exception;

public class SinkConnectException extends SinkException {

    public SinkConnectException(String message) {
        super(message);
    }

    public SinkConnectException(String message, Throwable cause) {
        super(message, cause);
    }
}
