package com.whaleal.third.mongo.sink.kafka.codec;

import com.whaleal.third.mongo.transfer.model.DdlEvent;
import com.whaleal.third.mongo.transfer.model.DdlType;
import com.whaleal.third.mongo.transfer.model.TransferEvent;
import com.whaleal.third.mongo.transfer.model.TransferSource;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ChangeStreamDocumentEncoderTest {

    @Test
    public void insertUsesFullDocumentAndDocumentKey() {
        Map<String, Object> after = new HashMap<String, Object>();
        after.put("_id", "507f1f77bcf86cd799439011");
        after.put("n", 1);
        TransferEvent event = TransferEvent.builder()
                .op("c")
                .after(after)
                .source(TransferSource.builder().db("demo").collection("orders").clusterTime(1_700_000_000_000L).build())
                .tsMs(1_700_000_000_000L)
                .build();

        BsonDocument doc = ChangeStreamDocumentEncoder.encode(event, "tgt", "tcoll");
        Assert.assertEquals("insert", doc.getString("operationType").getValue());
        Assert.assertEquals("demo", doc.getDocument("ns").getString("db").getValue());
        Assert.assertEquals("orders", doc.getDocument("ns").getString("coll").getValue());
        Assert.assertTrue(doc.containsKey("fullDocument"));
        Assert.assertTrue(doc.containsKey("documentKey"));
        Assert.assertEquals(1, doc.getDocument("fullDocument").getInt32("n").getValue());
    }

    @Test
    public void snapshotReadIsInsert() {
        Map<String, Object> after = new HashMap<String, Object>();
        after.put("_id", 1);
        TransferEvent event = TransferEvent.builder().op("r").after(after).build();
        BsonDocument doc = ChangeStreamDocumentEncoder.encode(event, "db", "coll");
        Assert.assertEquals("insert", doc.getString("operationType").getValue());
    }

    @Test
    public void operatorUpdateEmitsUpdateDescription() {
        Map<String, Object> set = new HashMap<String, Object>();
        set.put("name", "x");
        Map<String, Object> unset = new HashMap<String, Object>();
        unset.put("old", true);
        Map<String, Object> after = new HashMap<String, Object>();
        after.put("$set", set);
        after.put("$unset", unset);
        Map<String, Object> before = new HashMap<String, Object>();
        before.put("_id", 7);
        TransferEvent event = TransferEvent.builder().op("u").after(after).before(before).build();

        BsonDocument doc = ChangeStreamDocumentEncoder.encode(event, "db", "coll");
        Assert.assertEquals("update", doc.getString("operationType").getValue());
        Assert.assertTrue(doc.containsKey("updateDescription"));
        Assert.assertEquals("x", doc.getDocument("updateDescription")
                .getDocument("updatedFields").getString("name").getValue());
        Assert.assertEquals("old", doc.getDocument("updateDescription")
                .getArray("removedFields").get(0).asString().getValue());
        Assert.assertEquals(7, doc.getDocument("documentKey").getInt32("_id").getValue());
    }

    @Test
    public void fullDocumentUpdateIsReplace() {
        Map<String, Object> after = new HashMap<String, Object>();
        after.put("_id", 1);
        after.put("v", 2);
        TransferEvent event = TransferEvent.builder().op("u").after(after).build();
        BsonDocument doc = ChangeStreamDocumentEncoder.encode(event, "db", "coll");
        Assert.assertEquals("replace", doc.getString("operationType").getValue());
        Assert.assertTrue(doc.containsKey("fullDocument"));
        Assert.assertFalse(doc.containsKey("updateDescription"));
    }

    @Test
    public void deleteUsesDocumentKey() {
        Map<String, Object> before = new HashMap<String, Object>();
        before.put("_id", 9);
        TransferEvent event = TransferEvent.builder().op("d").before(before).build();
        BsonDocument doc = ChangeStreamDocumentEncoder.encode(event, "db", "coll");
        Assert.assertEquals("delete", doc.getString("operationType").getValue());
        Assert.assertEquals(9, doc.getDocument("documentKey").getInt32("_id").getValue());
        Assert.assertFalse(doc.containsKey("fullDocument"));
    }

    @Test
    public void dropDdl() {
        DdlEvent ddl = DdlEvent.builder()
                .type(DdlType.DROP_COLLECTION)
                .database("demo")
                .collection("orders")
                .command(new BsonDocument("drop", new BsonString("orders")))
                .build();
        BsonDocument doc = ChangeStreamDocumentEncoder.encodeDdl(ddl, "demo", "orders");
        Assert.assertEquals("drop", doc.getString("operationType").getValue());
        Assert.assertEquals("orders", doc.getDocument("ns").getString("coll").getValue());
    }

    @Test
    public void jsonRoundTripHasOperationType() {
        Map<String, Object> after = new HashMap<String, Object>();
        after.put("_id", 1);
        TransferEvent event = TransferEvent.builder().op("c").after(after).build();
        BsonDocument doc = ChangeStreamDocumentEncoder.encode(event, "db", "coll");
        String json = ChangeStreamDocumentEncoder.toJson(doc);
        Assert.assertTrue(json.contains("\"operationType\""));
        Assert.assertTrue(ChangeStreamDocumentEncoder.keyJson(doc).contains("_id"));
    }
}
