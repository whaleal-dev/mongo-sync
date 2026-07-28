package com.whaleal.third.mongo.source.oplog;

import com.mongodb.CursorType;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.whaleal.third.mongo.source.exception.SourceHistoryLostException;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

/**
 * 从 local.oplog.rs 获取增量条目。
 */
public class OplogFetcher {

    private static final String OPLOG_DB = "local";
    private static final String OPLOG_COLLECTION = "oplog.rs";

    private final MongoClient mongoClient;
    private final String database;
    private final String collection;
    private final OplogFormatVersion formatVersion;
    private final int batchSize;
    private final boolean includeFromMigrate;

    public OplogFetcher(MongoClient mongoClient,
                        String database,
                        String collection,
                        OplogFormatVersion formatVersion,
                        int batchSize,
                        boolean includeFromMigrate) {
        this.mongoClient = mongoClient;
        this.database = database;
        this.collection = collection;
        this.formatVersion = formatVersion;
        this.batchSize = batchSize;
        this.includeFromMigrate = includeFromMigrate;
    }

    public MongoCursor<BsonDocument> openCursor(BsonTimestamp startTsExclusive) {
        return openCursor(startTsExclusive, null);
    }

    /**
     * @param startTsExclusive 起点（不含）
     * @param endTsInclusive   终点（含）；非空时使用非 tailable 有界扫描，读完即结束
     */
    public MongoCursor<BsonDocument> openCursor(BsonTimestamp startTsExclusive, BsonTimestamp endTsInclusive) {
        MongoCollection<BsonDocument> oplog = mongoClient
                .getDatabase(OPLOG_DB)
                .getCollection(OPLOG_COLLECTION, BsonDocument.class);

        BsonTimestamp start = startTsExclusive != null ? startTsExclusive : new BsonTimestamp(0, 1);
        validateWindow(start);

        com.mongodb.client.FindIterable<BsonDocument> find = oplog.find(buildFilter(start, endTsInclusive))
                .projection(Projections.include("ts", "ns", "op", "o", "o2", "fromMigrate", "wall", "v", "t"))
                .sort(Sorts.ascending("$natural"))
                .batchSize(batchSize);

        if (endTsInclusive == null) {
            find = find.cursorType(CursorType.TailableAwait)
                    .noCursorTimeout(true)
                    .maxAwaitTime(2L, java.util.concurrent.TimeUnit.SECONDS);
        }
        return find.iterator();
    }

    public boolean shouldSkip(BsonDocument entry) {
        if (includeFromMigrate || !entry.containsKey("fromMigrate")) {
            return false;
        }
        org.bson.BsonValue value = entry.get("fromMigrate");
        return value != null && value.isBoolean() && value.asBoolean().getValue();
    }

    public BsonTimestamp readLatestTimestamp() {
        BsonDocument last = oplog().find()
                .sort(Sorts.descending("$natural"))
                .projection(Projections.include("ts"))
                .first();
        return last != null && last.containsKey("ts") ? last.getTimestamp("ts") : null;
    }

    public BsonTimestamp readEarliestTimestamp() {
        BsonDocument first = oplog().find()
                .sort(Sorts.ascending("$natural"))
                .projection(Projections.include("ts"))
                .first();
        return first != null && first.containsKey("ts") ? first.getTimestamp("ts") : null;
    }

    private void validateWindow(BsonTimestamp startTs) {
        if (startTs == null || startTs.getTime() <= 0) {
            return;
        }
        BsonTimestamp earliest = readEarliestTimestamp();
        if (earliest != null && startTs.compareTo(earliest) < 0) {
            throw new SourceHistoryLostException(
                    "Oplog history lost: start ts=" + startTs.getTime()
                            + " is earlier than oplog earliest ts=" + earliest.getTime());
        }
    }

    private Bson buildFilter(BsonTimestamp startTs, BsonTimestamp endTsInclusive) {
        List<Bson> nsFilters = new ArrayList<Bson>();
        nsFilters.add(Filters.eq("ns", database + "." + collection));
        nsFilters.add(Filters.eq("ns", database + ".$cmd"));
        if (formatVersion == OplogFormatVersion.V1) {
            nsFilters.add(Filters.eq("ns", database + ".system.indexes"));
        }
        List<Bson> parts = new ArrayList<Bson>();
        parts.add(Filters.gt("ts", startTs));
        if (endTsInclusive != null) {
            parts.add(Filters.lte("ts", endTsInclusive));
        }
        parts.add(Filters.or(nsFilters));
        return Filters.and(parts);
    }

    private MongoCollection<BsonDocument> oplog() {
        return mongoClient.getDatabase(OPLOG_DB).getCollection(OPLOG_COLLECTION, BsonDocument.class);
    }
}
