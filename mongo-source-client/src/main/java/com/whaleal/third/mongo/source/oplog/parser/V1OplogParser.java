package com.whaleal.third.mongo.source.oplog.parser;

import com.mongodb.client.MongoCollection;
import com.whaleal.third.mongo.source.config.MongoSourceConfig;
import com.whaleal.third.mongo.source.oplog.MongoVersion;
import com.whaleal.third.mongo.source.oplog.OplogFormatVersion;
import org.bson.BsonDocument;
import org.bson.BsonString;

import java.util.Map;

/**
 * V1（3.2 / 3.4）：建索引走 system.indexes 的 op=i。
 */
public class V1OplogParser extends AbstractOplogParser {

    public V1OplogParser(String database,
                         String collection,
                         MongoVersion mongoVersion,
                         MongoSourceConfig.FullDocumentMode fullDocumentMode,
                         MongoCollection<BsonDocument> sourceCollection) {
        super(database, collection, mongoVersion, OplogFormatVersion.V1, fullDocumentMode, sourceCollection);
    }

    @Override
    protected BsonDocument maybeNormalizeV1IndexInsert(BsonDocument entry) {
        String ns = entry.containsKey("ns") ? entry.getString("ns").getValue() : "";
        String op = entry.containsKey("op") ? entry.getString("op").getValue() : "";
        if (!"i".equals(op) || !systemIndexesNs.equals(ns)) {
            return entry;
        }
        BsonDocument o = getDocument(entry, "o");
        if (o == null || o.containsKey("_id") || !o.containsKey("ns")) {
            return entry;
        }
        String indexNs = o.getString("ns").getValue();
        if (!watchedNs.equals(indexNs)) {
            return entry;
        }
        String tableName = indexNs.substring(indexNs.indexOf('.') + 1);
        BsonDocument newO = new BsonDocument();
        for (Map.Entry<String, org.bson.BsonValue> e : o.entrySet()) {
            if (!"ns".equals(e.getKey())) {
                newO.put(e.getKey(), e.getValue());
            }
        }
        newO.put("createIndexes", new BsonString(tableName));

        BsonDocument normalized = new BsonDocument();
        for (Map.Entry<String, org.bson.BsonValue> e : entry.entrySet()) {
            if ("op".equals(e.getKey()) || "ns".equals(e.getKey()) || "o".equals(e.getKey())) {
                continue;
            }
            normalized.put(e.getKey(), e.getValue());
        }
        normalized.put("op", new BsonString("c"));
        normalized.put("ns", new BsonString(cmdNs));
        normalized.put("o", newO);
        return normalized;
    }
}
