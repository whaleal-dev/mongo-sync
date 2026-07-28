package com.whaleal.third.mongo.transfer.model;

/**
 * DDL / 索引事件类型（捕获方式无关）。
 */
public enum DdlType {
    CREATE_COLLECTION,
    DROP_COLLECTION,
    RENAME_COLLECTION,
    DROP_DATABASE,
    CREATE_INDEXES,
    DROP_INDEXES
}
