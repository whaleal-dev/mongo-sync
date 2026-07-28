package com.whaleal.third.mongo.sync.ns;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * 按 {@link NamespaceFilter} 发现源端待同步集合。
 */
public final class CollectionDiscovery {

    private CollectionDiscovery() {
    }

    public static List<NamespaceMapper.NsPair> discover(MongoClient sourceClient,
                                                        NamespaceFilter filter,
                                                        NamespaceMapper mapper) {
        if (sourceClient == null) {
            throw new IllegalArgumentException("sourceClient is required");
        }
        NamespaceFilter f = filter == null ? NamespaceFilter.empty() : filter;
        NamespaceMapper m = mapper == null ? NamespaceMapper.identity() : mapper;

        List<NamespaceMapper.NsPair> result = new ArrayList<NamespaceMapper.NsPair>();
        for (String dbName : sourceClient.listDatabaseNames()) {
            if (NamespaceFilter.isSystemDatabase(dbName)) {
                continue;
            }
            // 白名单仅含其它库时，可跳过本库扫描
            if (f.hasWhite() && !whiteTouchesDatabase(f, dbName)) {
                continue;
            }
            MongoDatabase db = sourceClient.getDatabase(dbName);
            for (Document info : db.listCollections()) {
                String coll = info.getString("name");
                if (coll == null || NamespaceFilter.isSystemCollection(coll)) {
                    continue;
                }
                String type = info.getString("type");
                if (type != null
                        && !"collection".equalsIgnoreCase(type)
                        && !"timeseries".equalsIgnoreCase(type)
                        && !"view".equalsIgnoreCase(type)) {
                    continue;
                }
                if (!f.accept(dbName, coll)) {
                    continue;
                }
                result.add(m.map(dbName, coll));
            }
        }
        return result;
    }

    private static boolean whiteTouchesDatabase(NamespaceFilter filter, String dbName) {
        for (String rule : filter.white()) {
            if (dbName.equals(rule) || rule.startsWith(dbName + ".")) {
                return true;
            }
        }
        return false;
    }
}
