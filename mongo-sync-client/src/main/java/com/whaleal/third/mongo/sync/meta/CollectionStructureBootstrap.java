package com.whaleal.third.mongo.sync.meta;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * 同步启动前：从源端读取集合定义（options + indexes），在 Sink 端创建集合/视图与索引。
 * <p>
 * 对齐 d2t 元数据预建：{@code MongoDBMetadata} → {@code ApplyMongoDBMetadata}。
 */
public final class CollectionStructureBootstrap {

    /** MongoDB NamespaceExists */
    private static final int ERR_NAMESPACE_EXISTS = 48;
    /** IndexOptionsConflict / IndexKeySpecsConflict 等「索引已存在」类错误 */
    private static final int ERR_INDEX_OPTIONS_CONFLICT = 85;
    private static final int ERR_INDEX_KEY_SPECS_CONFLICT = 86;

    private CollectionStructureBootstrap() {
    }

    /**
     * 确保 Sink 端具备与源端一致的集合结构。
     *
     * @param createCollection 是否创建集合/视图
     * @param createIndexes    是否创建非 _id 索引
     * @param skipTtlIndexes   为 true 时跳过带 expireAfterSeconds 的 TTL 索引（对齐 d2t）
     */
    public static void ensureSink(MongoClient sourceClient,
                                    MongoClient sinkClient,
                                    String sourceDatabase,
                                    String sourceCollection,
                                    String sinkDatabase,
                                    String sinkCollection,
                                    boolean createCollection,
                                    boolean createIndexes,
                                    boolean skipTtlIndexes) {
        if (sourceClient == null || sinkClient == null) {
            throw new IllegalArgumentException("source/sink MongoClient is required");
        }
        if (!createCollection && !createIndexes) {
            return;
        }
        Document collInfo = findCollectionInfo(sourceClient, sourceDatabase, sourceCollection);
        if (collInfo == null) {
            throw new IllegalStateException(
                    "source collection not found: " + sourceDatabase + "." + sourceCollection);
        }

        String type = collInfo.getString("type");
        if (type == null) {
            type = "collection";
        }

        MongoDatabase sinkDb = sinkClient.getDatabase(sinkDatabase);
        if ("view".equalsIgnoreCase(type)) {
            if (createCollection) {
                ensureView(sinkDb, sinkCollection, collInfo);
            }
        } else if ("collection".equalsIgnoreCase(type) || "timeseries".equalsIgnoreCase(type)) {
            if (createCollection) {
                ensureCollection(sinkDb, sinkCollection, collInfo);
            }
            if (createIndexes) {
                ensureIndexes(
                        sourceClient.getDatabase(sourceDatabase).getCollection(sourceCollection),
                        sinkDb.getCollection(sinkCollection),
                        skipTtlIndexes);
            }
        } else {
            System.err.println("[mongo-sync] skip bootstrap unsupported collection type=" + type
                    + " ns=" + sourceDatabase + "." + sourceCollection);
        }
    }

    private static Document findCollectionInfo(MongoClient client, String database, String collection) {
        return client.getDatabase(database)
                .listCollections()
                .filter(Filters.eq("name", collection))
                .first();
    }

    private static boolean sinkExists(MongoDatabase db, String collection) {
        Document info = db.listCollections().filter(Filters.eq("name", collection)).first();
        return info != null;
    }

    private static void ensureCollection(MongoDatabase sinkDb, String sinkCollection, Document collInfo) {
        if (sinkExists(sinkDb, sinkCollection)) {
            System.err.println("[mongo-sync] sink collection already exists, skip create: "
                    + sinkDb.getName() + "." + sinkCollection);
            return;
        }
        Document options = collInfo.get("options", Document.class);
        try {
            // 优先 runCommand 透传 options，覆盖 driver CreateCollectionOptions 未建模字段
            if (options != null && !options.isEmpty()) {
                Document createCmd = new Document("create", sinkCollection);
                for (String key : options.keySet()) {
                    createCmd.append(key, options.get(key));
                }
                System.err.println("[mongo-sync] create sink collection via command: "
                        + sinkDb.getName() + "." + sinkCollection + " options=" + options.toJson());
                sinkDb.runCommand(createCmd);
            } else {
                CreateCollectionOptions empty = new CreateCollectionOptions();
                System.err.println("[mongo-sync] create sink collection: "
                        + sinkDb.getName() + "." + sinkCollection);
                sinkDb.createCollection(sinkCollection, empty);
            }
        } catch (MongoCommandException e) {
            if (e.getErrorCode() == ERR_NAMESPACE_EXISTS) {
                return;
            }
            // runCommand 失败时回退到解析后的 CreateCollectionOptions
            if (options != null && !options.isEmpty()) {
                try {
                    CreateCollectionOptions parsed = MongoStructureParser.parseCreateCollectionOption(options);
                    sinkDb.createCollection(sinkCollection, parsed);
                    return;
                } catch (MongoCommandException e2) {
                    if (e2.getErrorCode() == ERR_NAMESPACE_EXISTS) {
                        return;
                    }
                    throw e2;
                }
            }
            throw e;
        }
    }

    private static void ensureView(MongoDatabase sinkDb, String sinkCollection, Document viewInfo) {
        if (sinkExists(sinkDb, sinkCollection)) {
            System.err.println("[mongo-sync] sink view already exists, skip create: "
                    + sinkDb.getName() + "." + sinkCollection);
            return;
        }
        Document options = viewInfo.get("options", Document.class);
        if (options == null) {
            throw new IllegalStateException("view options missing for " + sinkCollection);
        }
        String viewOn = options.get("viewOn") == null ? null : options.get("viewOn").toString();
        @SuppressWarnings("unchecked")
        List<Document> pipeline = options.get("pipeline", List.class);
        if (viewOn == null || pipeline == null) {
            throw new IllegalStateException("viewOn/pipeline required to create view " + sinkCollection);
        }
        CreateViewOptions createViewOptions = new CreateViewOptions();
        if (options.get("collation") != null) {
            createViewOptions.collation(
                    MongoStructureParser.parseCollation(options.get("collation", Document.class)));
        }
        System.err.println("[mongo-sync] create sink view: "
                + sinkDb.getName() + "." + sinkCollection + " on " + viewOn);
        try {
            sinkDb.createView(sinkCollection, viewOn, pipeline, createViewOptions);
        } catch (MongoCommandException e) {
            if (e.getErrorCode() == ERR_NAMESPACE_EXISTS) {
                return;
            }
            throw e;
        }
    }

    private static void ensureIndexes(MongoCollection<Document> source,
                                      MongoCollection<Document> sink,
                                      boolean skipTtlIndexes) {
        List<Document> indexes = source.listIndexes().into(new ArrayList<Document>());
        for (Document idx : indexes) {
            String name = idx.get("name") == null ? null : idx.get("name").toString();
            if (name == null || "_id_".equals(name)) {
                continue;
            }
            if (skipTtlIndexes && idx.containsKey("expireAfterSeconds")) {
                System.err.println("[mongo-sync] skip TTL index: " + name + " on "
                        + sink.getNamespace().getFullName());
                continue;
            }
            Document key = idx.get("key", Document.class);
            if (key == null || key.isEmpty()) {
                continue;
            }
            IndexOptions indexOptions = MongoStructureParser.parseIndexOptions(idx);
            try {
                System.err.println("[mongo-sync] create index " + name + " on "
                        + sink.getNamespace().getFullName() + " key=" + key.toJson());
                sink.createIndex(key, indexOptions);
            } catch (MongoCommandException e) {
                if (isIgnorableIndexError(e)) {
                    System.err.println("[mongo-sync] index already exists, skip: " + name
                            + " code=" + e.getErrorCode());
                    continue;
                }
                throw e;
            }
        }
    }

    private static boolean isIgnorableIndexError(MongoCommandException e) {
        int code = e.getErrorCode();
        if (code == ERR_INDEX_OPTIONS_CONFLICT || code == ERR_INDEX_KEY_SPECS_CONFLICT) {
            return true;
        }
        String msg = e.getErrorMessage();
        if (msg == null) {
            return false;
        }
        String lower = msg.toLowerCase();
        return lower.contains("already exists") || lower.contains("index already exists");
    }
}
