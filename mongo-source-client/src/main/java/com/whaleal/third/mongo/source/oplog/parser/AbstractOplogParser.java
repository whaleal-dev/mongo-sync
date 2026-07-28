package com.whaleal.third.mongo.source.oplog.parser;

import com.mongodb.client.MongoCollection;
import com.whaleal.third.mongo.source.config.MongoSourceConfig;
import com.whaleal.third.mongo.source.exception.TransferEventConvertException;
import com.whaleal.third.mongo.transfer.model.DdlEvent;
import com.whaleal.third.mongo.transfer.model.DdlType;
import com.whaleal.third.mongo.transfer.model.TransferEvent;
import com.whaleal.third.mongo.transfer.model.TransferSource;
import com.whaleal.third.mongo.source.oplog.BsonMaps;
import com.whaleal.third.mongo.source.oplog.MongoVersion;
import com.whaleal.third.mongo.source.oplog.OplogFormatVersion;
import com.whaleal.third.mongo.source.oplog.OplogParseResult;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.util.Map;

public abstract class AbstractOplogParser {

    protected final String database;
    protected final String collection;
    protected final String watchedNs;
    protected final String cmdNs;
    protected final String systemIndexesNs;
    protected final MongoVersion mongoVersion;
    protected final OplogFormatVersion formatVersion;
    protected final MongoSourceConfig.FullDocumentMode fullDocumentMode;
    protected final MongoCollection<BsonDocument> sourceCollection;

    protected AbstractOplogParser(String database,
                                  String collection,
                                  MongoVersion mongoVersion,
                                  OplogFormatVersion formatVersion,
                                  MongoSourceConfig.FullDocumentMode fullDocumentMode,
                                  MongoCollection<BsonDocument> sourceCollection) {
        this.database = database;
        this.collection = collection;
        this.watchedNs = database + "." + collection;
        this.cmdNs = database + ".$cmd";
        this.systemIndexesNs = database + ".system.indexes";
        this.mongoVersion = mongoVersion;
        this.formatVersion = formatVersion;
        this.fullDocumentMode = fullDocumentMode == null
                ? MongoSourceConfig.FullDocumentMode.DEFAULT
                : fullDocumentMode;
        this.sourceCollection = sourceCollection;
    }

    public final OplogParseResult parse(BsonDocument entry) {
        if (entry == null) {
            return OplogParseResult.skip(null);
        }
        BsonTimestamp ts = entry.containsKey("ts") ? entry.getTimestamp("ts") : null;
        String op = entry.containsKey("op") ? entry.getString("op").getValue() : null;
        if (op == null || "n".equals(op)) {
            return OplogParseResult.skip(ts);
        }

        BsonDocument normalized = maybeNormalizeV1IndexInsert(entry);
        String ns = normalized.containsKey("ns") ? normalized.getString("ns").getValue() : "";
        op = normalized.getString("op").getValue();

        if ("i".equals(op) || "u".equals(op) || "d".equals(op)) {
            if (!watchedNs.equals(ns)) {
                return OplogParseResult.skip(ts);
            }
            return parseCrud(normalized, op, ts);
        }

        if ("c".equals(op)) {
            return parseCommand(normalized, ns, ts);
        }
        return OplogParseResult.skip(ts);
    }

    /**
     * V1: {@code db.system.indexes} + op=i 且无 _id → 归一为 createIndexes cmd。
     */
    protected BsonDocument maybeNormalizeV1IndexInsert(BsonDocument entry) {
        return entry;
    }

    private OplogParseResult parseCrud(BsonDocument entry, String op, BsonTimestamp ts) {
        BsonDocument o = getDocument(entry, "o");
        BsonDocument o2 = getDocument(entry, "o2");

        Map<String, Object> before = null;
        Map<String, Object> after = null;
        String envelopeOp;

        switch (op) {
            case "i":
                envelopeOp = "c";
                after = BsonMaps.toMap(o);
                break;
            case "u":
                envelopeOp = "u";
                after = resolveUpdateAfter(normalizeUpdatePayload(o), o2);
                // 无 preImage 时用 o2（documentKey）填 before，供 Sink 定位 _id
                before = BsonMaps.toMap(o2);
                break;
            case "d":
                envelopeOp = "d";
                before = BsonMaps.toMap(o);
                break;
            default:
                return OplogParseResult.skip(ts);
        }

        long tsMs = ts != null ? ts.getTime() * 1000L : System.currentTimeMillis();
        TransferEvent event = TransferEvent.builder()
                .before(before)
                .op(envelopeOp)
                .after(after)
                .source(TransferSource.builder()
                        .db(database)
                        .collection(collection)
                        .clusterTime(tsMs)
                        .build())
                .tsMs(tsMs)
                .build();
        return OplogParseResult.crud(event, ts);
    }

    protected BsonDocument normalizeUpdatePayload(BsonDocument o) {
        if (o == null) {
            return null;
        }
        BsonDocument copy = new BsonDocument();
        for (Map.Entry<String, BsonValue> entry : o.entrySet()) {
            if ("$v".equals(entry.getKey())) {
                continue;
            }
            copy.put(entry.getKey(), entry.getValue());
        }
        return copy;
    }

    private Map<String, Object> resolveUpdateAfter(BsonDocument o, BsonDocument o2) {
        boolean needLookup = fullDocumentMode == MongoSourceConfig.FullDocumentMode.UPDATE_LOOKUP
                || fullDocumentMode == MongoSourceConfig.FullDocumentMode.REQUIRED
                || fullDocumentMode == MongoSourceConfig.FullDocumentMode.WHEN_AVAILABLE;

        if (needLookup && sourceCollection != null && o2 != null) {
            BsonDocument fullDoc = sourceCollection.find((Bson) o2).first();
            if (fullDoc != null) {
                return BsonMaps.toMap(fullDoc);
            }
            if (fullDocumentMode == MongoSourceConfig.FullDocumentMode.REQUIRED) {
                throw new TransferEventConvertException("Full document required but not found for oplog update, filter=" + o2.toJson());
            }
        }
        return BsonMaps.toMap(o);
    }

    private String readCommandCollection(BsonDocument o, String field) {
        if (o == null || !o.containsKey(field)) {
            return null;
        }
        org.bson.BsonValue value = o.get(field);
        if (value != null && value.isString()) {
            return value.asString().getValue();
        }
        return null;
    }

    private OplogParseResult parseCommand(BsonDocument entry, String ns, BsonTimestamp ts) {
        if (!cmdNs.equals(ns)) {
            return OplogParseResult.skip(ts);
        }
        BsonDocument o = getDocument(entry, "o");
        if (o == null) {
            return OplogParseResult.skip(ts);
        }

        // startIndexBuild: 未提交，只推进位点
        if (o.containsKey("startIndexBuild")) {
            return OplogParseResult.skip(ts);
        }

        DdlEvent ddl = null;
        if (o.containsKey("createIndexes")) {
            String coll = readCommandCollection(o, "createIndexes");
            if (coll == null || !collection.equals(coll)) {
                return OplogParseResult.skip(ts);
            }
            ddl = ddl(DdlType.CREATE_INDEXES, coll, o, ts, wallTime(entry));
        } else if (o.containsKey("commitIndexBuild")) {
            String coll = readCommandCollection(o, "commitIndexBuild");
            if (coll == null || !collection.equals(coll)) {
                return OplogParseResult.skip(ts);
            }
            BsonDocument normalized = new BsonDocument();
            normalized.put("createIndexes", o.get("commitIndexBuild"));
            if (o.containsKey("indexes")) {
                normalized.put("indexes", o.get("indexes"));
            }
            if (o.containsKey("indexBuildUUID")) {
                normalized.put("indexBuildUUID", o.get("indexBuildUUID"));
            }
            ddl = ddl(DdlType.CREATE_INDEXES, coll, normalized, ts, wallTime(entry));
        } else if (o.containsKey("dropIndexes")) {
            String coll = readCommandCollection(o, "dropIndexes");
            if (coll == null || !collection.equals(coll)) {
                return OplogParseResult.skip(ts);
            }
            ddl = ddl(DdlType.DROP_INDEXES, coll, o, ts, wallTime(entry));
        } else if (o.containsKey("create")) {
            String coll = readCommandCollection(o, "create");
            if (coll == null || !collection.equals(coll)) {
                return OplogParseResult.skip(ts);
            }
            ddl = ddl(DdlType.CREATE_COLLECTION, coll, o, ts, wallTime(entry));
        } else if (o.containsKey("drop")) {
            String coll = readCommandCollection(o, "drop");
            if (coll == null || !collection.equals(coll)) {
                return OplogParseResult.skip(ts);
            }
            ddl = ddl(DdlType.DROP_COLLECTION, coll, o, ts, wallTime(entry));
        } else if (o.containsKey("renameCollection")) {
            String from = o.getString("renameCollection").getValue();
            String to = o.containsKey("to") ? o.getString("to").getValue() : null;
            if (!watchedNs.equals(from) && (to == null || !watchedNs.equals(to))) {
                return OplogParseResult.skip(ts);
            }
            String coll = watchedNs.equals(from) ? collection
                    : (to != null && to.contains(".") ? to.substring(to.indexOf('.') + 1) : collection);
            ddl = ddl(DdlType.RENAME_COLLECTION, coll, o, ts, wallTime(entry));
        } else if (o.containsKey("dropDatabase")) {
            ddl = ddl(DdlType.DROP_DATABASE, null, o, ts, wallTime(entry));
        } else {
            return OplogParseResult.skip(ts);
        }
        return OplogParseResult.ddl(ddl, ts);
    }

    protected DdlEvent ddl(DdlType type, String coll, BsonDocument command, BsonTimestamp ts, Long wallTimeMs) {
        return DdlEvent.builder()
                .type(type)
                .database(database)
                .collection(coll)
                .command(command)
                .ts(ts)
                .wallTimeMs(wallTimeMs)
                .mongoVersion(mongoVersion == null ? null : mongoVersion.getRaw())
                .oplogFormat(formatVersion == null ? null : formatVersion.name())
                .build();
    }

    protected static BsonDocument getDocument(BsonDocument parent, String field) {
        if (parent == null || !parent.containsKey(field) || !parent.get(field).isDocument()) {
            return null;
        }
        return parent.getDocument(field);
    }

    protected static Long wallTime(BsonDocument entry) {
        if (entry != null && entry.containsKey("wall") && entry.get("wall").isDateTime()) {
            return entry.getDateTime("wall").getValue();
        }
        return null;
    }
}
