package com.whaleal.third.mongo.source.oplog.parser;

import com.mongodb.client.MongoCollection;
import com.whaleal.third.mongo.source.config.MongoSourceConfig;
import com.whaleal.third.mongo.source.oplog.MongoVersion;
import com.whaleal.third.mongo.source.oplog.OplogFormatVersion;
import org.bson.BsonDocument;

public final class OplogParserFactory {

    private OplogParserFactory() {
    }

    public static AbstractOplogParser create(String database,
                                             String collection,
                                             MongoVersion mongoVersion,
                                             MongoSourceConfig.FullDocumentMode fullDocumentMode,
                                             MongoCollection<BsonDocument> sourceCollection) {
        OplogFormatVersion format = mongoVersion.toOplogFormat();
        switch (format) {
            case V1:
                return new V1OplogParser(database, collection, mongoVersion, fullDocumentMode, sourceCollection);
            case V2:
                return new V2OplogParser(database, collection, mongoVersion, fullDocumentMode, sourceCollection);
            case V3:
                return new V3OplogParser(database, collection, mongoVersion, fullDocumentMode, sourceCollection);
            default:
                throw new IllegalStateException("Unsupported oplog format: " + format);
        }
    }
}
