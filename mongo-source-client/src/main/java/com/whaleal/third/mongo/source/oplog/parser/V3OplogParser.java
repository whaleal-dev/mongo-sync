package com.whaleal.third.mongo.source.oplog.parser;

import com.mongodb.client.MongoCollection;
import com.whaleal.third.mongo.source.config.MongoSourceConfig;
import com.whaleal.third.mongo.source.oplog.MongoVersion;
import com.whaleal.third.mongo.source.oplog.OplogDiffNormalizer;
import com.whaleal.third.mongo.source.oplog.OplogFormatVersion;
import org.bson.BsonDocument;

/**
 * V3（5.0 / 6.0）：update 可能为 $v:2 + diff，归一为 $set/$unset。
 */
public class V3OplogParser extends AbstractOplogParser {

    public V3OplogParser(String database,
                         String collection,
                         MongoVersion mongoVersion,
                         MongoSourceConfig.FullDocumentMode fullDocumentMode,
                         MongoCollection<BsonDocument> sourceCollection) {
        super(database, collection, mongoVersion, OplogFormatVersion.V3, fullDocumentMode, sourceCollection);
    }

    @Override
    protected BsonDocument normalizeUpdatePayload(BsonDocument o) {
        return OplogDiffNormalizer.normalizeUpdateDocument(o);
    }
}
