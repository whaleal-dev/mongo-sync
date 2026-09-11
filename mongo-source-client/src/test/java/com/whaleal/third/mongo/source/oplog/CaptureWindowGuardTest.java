package com.whaleal.third.mongo.source.oplog;

import org.bson.BsonTimestamp;
import org.junit.Assert;
import org.junit.Test;

public class CaptureWindowGuardTest {

    @Test
    public void remainingSecondsIsAnchorMinusEarliest() {
        BsonTimestamp earliest = new BsonTimestamp(1_000, 1);
        BsonTimestamp anchor = new BsonTimestamp(4_600, 1);
        Assert.assertEquals(Long.valueOf(3600L), CaptureWindowGuard.remainingSeconds(anchor, earliest));
    }

    @Test
    public void shouldWarnWhenAtOrBelowThreshold() {
        Assert.assertTrue(CaptureWindowGuard.shouldWarn(3600L, 3600));
        Assert.assertTrue(CaptureWindowGuard.shouldWarn(100L, 3600));
        Assert.assertFalse(CaptureWindowGuard.shouldWarn(3601L, 3600));
        Assert.assertFalse(CaptureWindowGuard.shouldWarn(100L, 0));
        Assert.assertFalse(CaptureWindowGuard.shouldWarn(null, 3600));
    }

    @Test
    public void nullInputsYieldNullRemaining() {
        Assert.assertNull(CaptureWindowGuard.remainingSeconds(null, new BsonTimestamp(1, 1)));
        Assert.assertNull(CaptureWindowGuard.remainingSeconds(new BsonTimestamp(1, 1), null));
    }
}
