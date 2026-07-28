package com.whaleal.third.mongo.sink.exception;

public class SinkWriteException extends SinkException {

    public SinkWriteException(String message) {
        super(message);
    }

    public SinkWriteException(String message, Throwable cause) {
        super(message, cause);
    }
}
