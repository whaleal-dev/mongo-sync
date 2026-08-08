package com.whaleal.third.mongo.source.full;

import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;

public class FullSyncRangeSplitterTest {

    @Test
    public void typedRangeFilterIncludesTypeAndBounds() {
        FullSyncRangeSplitter.IdRange range =
                new FullSyncRangeSplitter.IdRange("a", "z", false, 2);
        Document doc = (Document) range.toFilter();
        Document id = doc.get("_id", Document.class);
        Assert.assertNotNull(id);
        Assert.assertEquals(2, id.getInteger("$type").intValue());
        Assert.assertEquals("a", id.getString("$gte"));
        Assert.assertEquals("z", id.getString("$lt"));
    }

    @Test
    public void maxRangeUsesClosedUpperBound() {
        FullSyncRangeSplitter.IdRange range =
                new FullSyncRangeSplitter.IdRange("a", "z", true, 2);
        Document id = ((Document) range.toFilter()).get("_id", Document.class);
        Assert.assertEquals("z", id.getString("$lte"));
        Assert.assertNull(id.get("$lt"));
    }

    @Test
    public void allRangeHasEmptyFilter() {
        Document doc = (Document) FullSyncRangeSplitter.IdRange.all().toFilter();
        Assert.assertTrue(doc.isEmpty());
    }
}
