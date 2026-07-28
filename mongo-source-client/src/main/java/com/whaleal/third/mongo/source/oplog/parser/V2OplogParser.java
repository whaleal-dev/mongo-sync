package com.whaleal.third.mongo.source.oplog.parser;

import com.mongodb.client.MongoCollection;
import com.whaleal.third.mongo.source.config.MongoSourceConfig;
import com.whaleal.third.mongo.source.oplog.MongoVersion;
import com.whaleal.third.mongo.source.oplog.OplogFormatVersion;
import org.bson.BsonDocument;

/**
 * V2（3.6 / 4.0 / 4.4）：索引走 $cmd.createIndexes；update 常见 $v:1 + $set。
 */
public class V2OplogParser extends AbstractOplogParser {

    public V2OplogParser(String database,
                         String collection,
                         MongoVersion mongoVersion,
                         MongoSourceConfig.FullDocumentMode fullDocumentMode,
                         MongoCollection<BsonDocument> sourceCollection) {
        super(database, collection, mongoVersion, OplogFormatVersion.V2, fullDocumentMode, sourceCollection);
    }
}
