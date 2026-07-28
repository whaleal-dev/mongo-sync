package com.whaleal.third.mongo.sink.exception;

public class SinkException extends RuntimeException {

    public SinkException(String message) {
        super(message);
    }

    public SinkException(String message, Throwable cause) {
        super(message, cause);
    }
}
