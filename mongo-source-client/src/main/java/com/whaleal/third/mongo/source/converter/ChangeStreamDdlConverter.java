package com.whaleal.third.mongo.source.converter;

import com.whaleal.third.mongo.source.exception.TransferEventConvertException;
import com.whaleal.third.mongo.transfer.model.DdlEvent;
import com.whaleal.third.mongo.transfer.model.DdlType;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * ChangeStream DDL → {@link DdlEvent}（与 Oplog DDL 出口对齐）。
 */
public final class ChangeStreamDdlConverter {

    private static final Set<String> DDL_OPS = new HashSet<String>(Arrays.asList(
            "create", "createIndexes", "drop", "dropDatabase", "dropIndexes", "rename"
    ));

    private final String database;
    private final String collection;

    public ChangeStreamDdlConverter(String database, String collection) {
        this.database = database;
        this.collection = collection;
    }

    public static boolean isDdlOperation(String operationType) {
        return operationType != null && DDL_OPS.contains(operationType);
    }

    public static boolean isIgnorable(String operationType) {
        return "invalidate".equals(operationType);
    }

    public DdlEvent convert(BsonDocument changeStreamDocument) {
        try {
            String operationType = changeStreamDocument.getString("operationType").getValue();
            BsonTimestamp clusterTime = changeStreamDocument.getTimestamp("clusterTime", null);
            BsonDocument ns = changeStreamDocument.getDocument("ns", null);
            BsonDocument operationDescription = changeStreamDocument.getDocument("operationDescription", null);

            String db = database;
            String coll = collection;
            if (ns != null) {
                if (ns.containsKey("db") && ns.get("db").isString()) {
                    db = ns.getString("db").getValue();
                }
                if (ns.containsKey("coll") && ns.get("coll").isString()) {
                    coll = ns.getString("coll").getValue();
                }
            }

            DdlType type;
            BsonDocument command;
            switch (operationType) {
                case "create":
                    type = DdlType.CREATE_COLLECTION;
                    command = buildCreateCommand(coll, operationDescription);
                    break;
                case "createIndexes":
                    type = DdlType.CREATE_INDEXES;
                    command = buildCreateIndexesCommand(coll, operationDescription);
                    break;
                case "dropIndexes":
                    type = DdlType.DROP_INDEXES;
                    command = buildDropIndexesCommand(coll, operationDescription);
                    break;
                case "drop":
                    type = DdlType.DROP_COLLECTION;
                    command = new BsonDocument("drop", new BsonString(coll));
                    break;
                case "dropDatabase":
                    type = DdlType.DROP_DATABASE;
                    command = new BsonDocument("dropDatabase", new org.bson.BsonInt32(1));
                    coll = null;
                    break;
                case "rename":
                    type = DdlType.RENAME_COLLECTION;
                    command = buildRenameCommand(db, coll, changeStreamDocument, operationDescription);
                    break;
                default:
                    return null;
            }

            return DdlEvent.builder()
                    .type(type)
                    .database(db)
                    .collection(coll)
                    .command(command)
                    .ts(clusterTime)
                    .wallTimeMs(clusterTime != null ? clusterTime.getTime() * 1000L : System.currentTimeMillis())
                    .build();
        } catch (Exception e) {
            throw new TransferEventConvertException("Failed to convert change stream ddl", e);
        }
    }

    private static BsonDocument buildCreateCommand(String coll, BsonDocument operationDescription) {
        BsonDocument command = new BsonDocument("create", new BsonString(coll));
        if (operationDescription != null) {
            for (String key : operationDescription.keySet()) {
                if (!"create".equals(key)) {
                    command.put(key, operationDescription.get(key));
                }
            }
        }
        return command;
    }

    private static BsonDocument buildCreateIndexesCommand(String coll, BsonDocument operationDescription) {
        BsonDocument command = new BsonDocument("createIndexes", new BsonString(coll));
        if (operationDescription != null) {
            BsonValue indexes = operationDescription.get("indexes");
            if (indexes != null) {
                command.put("indexes", indexes);
            } else {
                for (String key : operationDescription.keySet()) {
                    if (!"createIndexes".equals(key)) {
                        command.put(key, operationDescription.get(key));
                    }
                }
            }
        }
        return command;
    }

    private static BsonDocument buildDropIndexesCommand(String coll, BsonDocument operationDescription) {
        BsonDocument command = new BsonDocument("dropIndexes", new BsonString(coll));
        if (operationDescription != null) {
            if (operationDescription.containsKey("index")) {
                command.put("index", operationDescription.get("index"));
            } else if (operationDescription.containsKey("indexes")) {
                BsonValue indexes = operationDescription.get("indexes");
                if (indexes.isArray() && indexes.asArray().size() == 1) {
                    command.put("index", indexes.asArray().get(0));
                } else {
                    command.put("index", indexes);
                }
            } else {
                for (String key : operationDescription.keySet()) {
                    if (!"dropIndexes".equals(key)) {
                        command.put(key, operationDescription.get(key));
                    }
                }
            }
        }
        return command;
    }

    private static BsonDocument buildRenameCommand(String db,
                                                   String coll,
                                                   BsonDocument changeStreamDocument,
                                                   BsonDocument operationDescription) {
        String from = db + "." + coll;
        String to = null;
        BsonDocument toNs = changeStreamDocument.getDocument("to", null);
        if (toNs == null) {
            toNs = changeStreamDocument.getDocument("destinationNamespace", null);
        }
        if (toNs != null
                && toNs.containsKey("db") && toNs.get("db").isString()
                && toNs.containsKey("coll") && toNs.get("coll").isString()) {
            to = toNs.getString("db").getValue() + "." + toNs.getString("coll").getValue();
        }
        if (to == null && operationDescription != null && operationDescription.containsKey("to")) {
            BsonValue toVal = operationDescription.get("to");
            if (toVal.isDocument()) {
                BsonDocument d = toVal.asDocument();
                if (d.containsKey("db") && d.containsKey("coll")) {
                    to = d.getString("db").getValue() + "." + d.getString("coll").getValue();
                }
            } else if (toVal.isString()) {
                to = toVal.asString().getValue();
            }
        }
        if (to == null) {
            throw new TransferEventConvertException("rename event missing destination namespace");
        }
        BsonDocument command = new BsonDocument();
        command.put("renameCollection", new BsonString(from));
        command.put("to", new BsonString(to));
        if (operationDescription != null && operationDescription.containsKey("dropTarget")) {
            command.put("dropTarget", operationDescription.get("dropTarget"));
        }
        return command;
    }
}
