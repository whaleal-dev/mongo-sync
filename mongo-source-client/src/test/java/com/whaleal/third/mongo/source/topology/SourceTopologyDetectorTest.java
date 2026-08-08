package com.whaleal.third.mongo.source.topology;

import com.whaleal.third.mongo.source.config.CaptureMode;
import com.whaleal.third.mongo.source.config.SyncMode;
import com.whaleal.third.mongo.source.oplog.MongoVersion;
import org.junit.Assert;
import org.junit.Test;

public class SourceTopologyDetectorTest {

    @Test
    public void autoReplicaSetUsesChangeStreamWhenAvailable() {
        CaptureMode mode = SourceTopologyDetector.resolveCaptureMode(
                CaptureMode.AUTO, SourceTopology.REPLICA_SET, MongoVersion.parse("4.4.0"), true);
        Assert.assertEquals(CaptureMode.CHANGE_STREAM, mode);
    }

    @Test
    public void autoReplicaSetFallsBackToOplogBelow36() {
        CaptureMode mode = SourceTopologyDetector.resolveCaptureMode(
                CaptureMode.AUTO, SourceTopology.REPLICA_SET, MongoVersion.parse("3.4.24"), true);
        Assert.assertEquals(CaptureMode.OPLOG, mode);
    }

    @Test
    public void autoShardingUsesChangeStreamWhenAvailable() {
        CaptureMode mode = SourceTopologyDetector.resolveCaptureMode(
                CaptureMode.AUTO, SourceTopology.SHARDING, MongoVersion.parse("4.4.0"), true);
        Assert.assertEquals(CaptureMode.CHANGE_STREAM, mode);
    }

    @Test
    public void autoShardingBelow36ThrowsForIncremental() {
        try {
            SourceTopologyDetector.resolveCaptureMode(
                    CaptureMode.AUTO, SourceTopology.SHARDING, MongoVersion.parse("3.4.24"), true);
            Assert.fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage().contains("ChangeStream"));
        }
    }

    @Test
    public void explicitOplogOnShardingRejected() {
        try {
            SourceTopologyDetector.validateReadCapability(
                    SourceTopology.SHARDING, CaptureMode.OPLOG, SyncMode.FULL_AND_INCREMENTAL);
            Assert.fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage().contains("multi-shard OPLOG"));
        }
    }

    @Test
    public void oplogOnReplicaSetAllowed() {
        SourceTopologyDetector.validateReadCapability(
                SourceTopology.REPLICA_SET, CaptureMode.OPLOG, SyncMode.INCREMENTAL);
    }
}
