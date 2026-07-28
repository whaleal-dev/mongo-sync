package com.whaleal.third.mongo.sink.writer;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.whaleal.third.mongo.sink.config.MongoSinkConfig;
import com.whaleal.third.mongo.sink.exception.SinkWriteException;
import com.whaleal.third.mongo.transfer.model.DdlEvent;
import com.whaleal.third.mongo.transfer.model.DdlType;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;

/**
 * 在目标库执行 DDL。只识别 {@link DdlEvent} / {@link DdlType}。
 */
public final class DdlApplier {

    private final MongoClient mongoClient;
    private final MongoSinkConfig config;

    public DdlApplier(MongoClient mongoClient, MongoSinkConfig config) {
        this.mongoClient = mongoClient;
        this.config = config;
    }

    public void apply(DdlEvent event) {
        if (event == null || event.getType() == null) {
            return;
        }
        String database = firstNonBlank(event.getDatabase(), config.getDatabase());
        String collection = firstNonBlank(event.getCollection(), config.getCollection());
        try {
            switch (event.getType()) {
                case CREATE_INDEXES:
                    runCommand(database, rewriteCollectionField(event.getCommand(), "createIndexes", collection));
                    break;
                case DROP_INDEXES:
                    runCommand(database, rewriteCollectionField(event.getCommand(), "dropIndexes", collection));
                    break;
                case CREATE_COLLECTION:
                    if (event.getCommand() != null && !event.getCommand().isEmpty()) {
                        runCommand(database, rewriteCollectionField(event.getCommand(), "create", collection));
                    } else {
                        mongoClient.getDatabase(database).createCollection(collection);
                    }
                    break;
                case DROP_COLLECTION:
                    mongoClient.getDatabase(database).getCollection(collection).drop();
                    break;
                case DROP_DATABASE:
                    mongoClient.getDatabase(database).drop();
                    break;
                case RENAME_COLLECTION:
                    runRename(event.getCommand(), database, collection);
                    break;
                default:
                    throw new SinkWriteException("unsupported ddl type: " + event.getType());
            }
        } catch (SinkWriteException e) {
            throw e;
        } catch (MongoException e) {
            throw new SinkWriteException("ddl apply failed: " + event.getType(), e);
        } catch (Exception e) {
            throw new SinkWriteException("ddl apply failed: " + event.getType(), e);
        }
    }

    private void runCommand(String database, BsonDocument command) {
        if (command == null || command.isEmpty()) {
            throw new SinkWriteException("ddl command is required");
        }
        MongoDatabase db = mongoClient.getDatabase(database);
        db.runCommand(command, Document.class);
    }

    private void runRename(BsonDocument command, String database, String collection) {
        BsonDocument cmd = command == null ? null : command.clone();
        if (cmd == null || !cmd.containsKey("renameCollection")) {
            throw new SinkWriteException("renameCollection command is required");
        }
        String to = cmd.containsKey("to") && cmd.get("to").isString()
                ? cmd.getString("to").getValue()
                : null;
        String targetFrom = database + "." + collection;
        String targetTo;
        if (to != null && to.contains(".")) {
            String toColl = to.substring(to.indexOf('.') + 1);
            targetTo = database + "." + toColl;
        } else if (to != null) {
            targetTo = database + "." + to;
        } else {
            throw new SinkWriteException("renameCollection missing to");
        }
        cmd.put("renameCollection", new BsonString(targetFrom));
        cmd.put("to", new BsonString(targetTo));
        mongoClient.getDatabase("admin").runCommand(cmd, Document.class);
    }

    private static BsonDocument rewriteCollectionField(BsonDocument command, String field, String collection) {
        if (command == null) {
            return null;
        }
        BsonDocument copy = command.clone();
        copy.put(field, new BsonString(collection));
        return copy;
    }

    private static String firstNonBlank(String value, String fallback) {
        if (value == null || value.trim().isEmpty()) {
            return fallback;
        }
        return value;
    }
}
