package com.whaleal.third.mongo.source.exception;

public class SourceOffsetException extends SourceException {

    public SourceOffsetException(String message) {
        super(message);
    }

    public SourceOffsetException(String message, Throwable cause) {
        super(message, cause);
    }
}
