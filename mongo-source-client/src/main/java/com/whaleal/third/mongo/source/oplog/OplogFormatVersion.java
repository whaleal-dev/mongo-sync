package com.whaleal.third.mongo.source.oplog;

/**
 * Oplog 格式族（与 d2t 文档一致）。
 * <ul>
 *   <li>V1 — MongoDB 3.2 / 3.4</li>
 *   <li>V2 — MongoDB 3.6 / 4.0 / 4.4</li>
 *   <li>V3 — MongoDB 5.0 / 6.0</li>
 * </ul>
 */
public enum OplogFormatVersion {
    V1,
    V2,
    V3
}
