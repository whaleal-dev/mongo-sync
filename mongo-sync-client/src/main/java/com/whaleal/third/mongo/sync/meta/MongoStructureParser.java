package com.whaleal.third.mongo.sync.meta;

import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.TimeSeriesGranularity;
import com.mongodb.client.model.TimeSeriesOptions;
import com.mongodb.client.model.ValidationOptions;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

/**
 * 解析 listCollections / listIndexes 返回的 Document，对齐 d2t {@code ParserMongoStructureUtil}。
 */
final class MongoStructureParser {

    private MongoStructureParser() {
    }

    static IndexOptions parseIndexOptions(Document o) {
        IndexOptions indexOptions = new IndexOptions();
        if (o.get("name") != null) {
            indexOptions.name(o.get("name").toString());
        }
        if (o.get("unique") != null) {
            indexOptions.unique(Boolean.parseBoolean(o.get("unique").toString()));
        }
        if (o.get("partialFilterExpression") != null) {
            indexOptions.partialFilterExpression((Bson) o.get("partialFilterExpression"));
        }
        if (o.get("sparse") != null) {
            indexOptions.sparse(Boolean.parseBoolean(o.get("sparse").toString()));
        }
        if (o.get("expireAfterSeconds") != null) {
            long expireAfter = ((Double) Double.parseDouble(o.get("expireAfterSeconds").toString())).longValue();
            indexOptions.expireAfter(expireAfter, TimeUnit.SECONDS);
        }
        if (o.get("hidden") != null) {
            indexOptions.hidden(Boolean.parseBoolean(o.get("hidden").toString()));
        }
        if (o.get("storageEngine") != null) {
            indexOptions.storageEngine((Bson) o.get("storageEngine"));
        }
        if (o.get("collation") != null) {
            indexOptions.collation(parseCollation((Document) o.get("collation")));
        }
        if (o.get("weights") != null) {
            indexOptions.weights((Bson) o.get("weights"));
        }
        if (o.get("textIndexVersion") != null) {
            indexOptions.textVersion(((Double) Double.parseDouble(o.get("textIndexVersion").toString())).intValue());
        }
        if (o.get("default_language") != null) {
            indexOptions.defaultLanguage(o.get("default_language").toString());
        }
        if (o.get("language_override") != null) {
            indexOptions.languageOverride(o.get("language_override").toString());
        }
        if (o.get("wildcardProjection") != null) {
            indexOptions.wildcardProjection((Bson) o.get("wildcardProjection"));
        }
        if (o.get("bucketSize") != null) {
            indexOptions.bucketSize(Double.parseDouble(o.get("bucketSize").toString()));
        }
        if (o.get("bits") != null) {
            indexOptions.bits(((Double) Double.parseDouble(o.get("bits").toString())).intValue());
        }
        if (o.get("max") != null) {
            indexOptions.max(Double.parseDouble(o.get("max").toString()));
        }
        if (o.get("min") != null) {
            indexOptions.min(Double.parseDouble(o.get("min").toString()));
        }
        if (o.get("2dsphereIndexVersion") != null) {
            indexOptions.sphereVersion(
                    ((Double) Double.parseDouble(o.get("2dsphereIndexVersion").toString())).intValue());
        }
        // 对齐 d2t：后台建索引，降低对在线写入影响
        indexOptions.background(true);
        return indexOptions;
    }

    static CreateCollectionOptions parseCreateCollectionOption(Document options) {
        CreateCollectionOptions collectionOptions = new CreateCollectionOptions();
        if (options == null || options.isEmpty()) {
            return collectionOptions;
        }
        if (options.get("validator") != null) {
            collectionOptions.validationOptions(
                    new ValidationOptions().validator(options.get("validator", Document.class)));
        }
        if (options.get("expireAfterSeconds") != null) {
            collectionOptions.expireAfter(
                    ((Double) Double.parseDouble(options.get("expireAfterSeconds").toString())).longValue(),
                    TimeUnit.SECONDS);
        }
        if (options.get("timeseries") != null) {
            Document timeseries = options.get("timeseries", Document.class);
            TimeSeriesOptions timeSeriesOptions = new TimeSeriesOptions(timeseries.getString("timeField"));
            if (timeseries.getString("metaField") != null) {
                timeSeriesOptions.metaField(timeseries.getString("metaField"));
            }
            Object granularity = timeseries.get("granularity");
            if (granularity != null) {
                String g = granularity.toString();
                if ("HOURS".equalsIgnoreCase(g)) {
                    timeSeriesOptions.granularity(TimeSeriesGranularity.HOURS);
                } else if ("MINUTES".equalsIgnoreCase(g)) {
                    timeSeriesOptions.granularity(TimeSeriesGranularity.MINUTES);
                } else if ("SECONDS".equalsIgnoreCase(g)) {
                    timeSeriesOptions.granularity(TimeSeriesGranularity.SECONDS);
                }
            }
            collectionOptions.timeSeriesOptions(timeSeriesOptions);
        }
        if (options.get("capped") != null && Boolean.parseBoolean(options.get("capped").toString())) {
            collectionOptions.capped(true);
        }
        if (options.get("size") != null) {
            collectionOptions.sizeInBytes(Long.parseLong(options.get("size").toString()));
        }
        if (options.get("max") != null) {
            collectionOptions.maxDocuments(Long.parseLong(options.get("max").toString()));
        }
        if (options.get("collation") != null) {
            collectionOptions.collation(parseCollation(options.get("collation", Document.class)));
        }
        return collectionOptions;
    }

    static Collation parseCollation(Document collation) {
        Collation.Builder collationBuilder = Collation.builder();
        if (collation.get("locale") != null) {
            collationBuilder.locale(collation.getString("locale"));
        }
        if (collation.get("caseLevel") != null) {
            collationBuilder.caseLevel(Boolean.parseBoolean(collation.get("caseLevel").toString()));
        }
        if (collation.get("caseFirst") != null) {
            collationBuilder.collationCaseFirst(CollationCaseFirst.fromString(collation.getString("caseFirst")));
        }
        if (collation.get("strength") != null) {
            int strength = collation.get("strength") instanceof Number
                    ? ((Number) collation.get("strength")).intValue()
                    : Integer.parseInt(collation.get("strength").toString());
            collationBuilder.collationStrength(CollationStrength.fromInt(strength));
        }
        if (collation.get("numericOrdering") != null) {
            collationBuilder.numericOrdering(Boolean.parseBoolean(collation.get("numericOrdering").toString()));
        }
        if (collation.get("alternate") != null) {
            collationBuilder.collationAlternate(CollationAlternate.fromString(collation.getString("alternate")));
        }
        if (collation.get("maxVariable") != null) {
            collationBuilder.collationMaxVariable(CollationMaxVariable.fromString(collation.getString("maxVariable")));
        }
        if (collation.get("normalization") != null) {
            collationBuilder.normalization(Boolean.parseBoolean(collation.get("normalization").toString()));
        }
        if (collation.get("backwards") != null) {
            collationBuilder.backwards(Boolean.parseBoolean(collation.get("backwards").toString()));
        }
        return collationBuilder.build();
    }
}
