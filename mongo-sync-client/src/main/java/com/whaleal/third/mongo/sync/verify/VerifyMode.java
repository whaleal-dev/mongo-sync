package com.whaleal.third.mongo.sync.verify;

/**
 * 校验深度。
 */
public enum VerifyMode {
    /** 仅比对文档数 */
    COUNT,
    /** 比对 _id 集合（缺源/缺目标） */
    ID,
    /** _id + 文档内容（可忽略字段） */
    FULL
}
