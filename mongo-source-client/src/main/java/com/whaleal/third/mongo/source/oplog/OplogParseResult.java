package com.whaleal.third.mongo.source.oplog;

import com.whaleal.third.mongo.transfer.model.DdlEvent;
import com.whaleal.third.mongo.transfer.model.TransferEvent;
import org.bson.BsonTimestamp;

/**
 * Oplog 解析结果：文档变更或 DDL（含索引）。
 */
public final class OplogParseResult {

    public enum Kind {
        SKIP,
        CRUD,
        DDL
    }

    private final Kind kind;
    private final TransferEvent sourceEvent;
    private final DdlEvent ddlEvent;
    private final BsonTimestamp ts;

    private OplogParseResult(Kind kind, TransferEvent sourceEvent, DdlEvent ddlEvent, BsonTimestamp ts) {
        this.kind = kind;
        this.sourceEvent = sourceEvent;
        this.ddlEvent = ddlEvent;
        this.ts = ts;
    }

    public static OplogParseResult skip(BsonTimestamp ts) {
        return new OplogParseResult(Kind.SKIP, null, null, ts);
    }

    public static OplogParseResult crud(TransferEvent event, BsonTimestamp ts) {
        return new OplogParseResult(Kind.CRUD, event, null, ts);
    }

    public static OplogParseResult ddl(DdlEvent event, BsonTimestamp ts) {
        return new OplogParseResult(Kind.DDL, null, event, ts);
    }

    public Kind getKind() {
        return kind;
    }

    public TransferEvent getTransferEvent() {
        return sourceEvent;
    }

    public DdlEvent getDdlEvent() {
        return ddlEvent;
    }

    public BsonTimestamp getTs() {
        return ts;
    }
}
