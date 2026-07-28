package com.whaleal.third.mongo.sync.sdk;

/**
 * 迁移进度快照。
 */
public final class MigrationProgress {

    private final MigrationState state;
    private final boolean canCommit;
    private final boolean fullSyncComplete;
    private final long snapshotEvents;
    private final long incrementalEvents;
    private final long ddlEvents;
    private final long inflightEvents;
    private final int shardSourceCount;
    private final Long lastEventTsMs;
    private final long startedAtMs;
    private final Long committedAtMs;
    private final String detail;

    public MigrationProgress(MigrationState state,
                             boolean canCommit,
                             boolean fullSyncComplete,
                             long snapshotEvents,
                             long incrementalEvents,
                             long ddlEvents,
                             long inflightEvents,
                             int shardSourceCount,
                             Long lastEventTsMs,
                             long startedAtMs,
                             Long committedAtMs,
                             String detail) {
        this.state = state;
        this.canCommit = canCommit;
        this.fullSyncComplete = fullSyncComplete;
        this.snapshotEvents = snapshotEvents;
        this.incrementalEvents = incrementalEvents;
        this.ddlEvents = ddlEvents;
        this.inflightEvents = inflightEvents;
        this.shardSourceCount = shardSourceCount;
        this.lastEventTsMs = lastEventTsMs;
        this.startedAtMs = startedAtMs;
        this.committedAtMs = committedAtMs;
        this.detail = detail;
    }

    public MigrationState getState() {
        return state;
    }

    public boolean isCanCommit() {
        return canCommit;
    }

    public boolean isFullSyncComplete() {
        return fullSyncComplete;
    }

    public long getSnapshotEvents() {
        return snapshotEvents;
    }

    public long getIncrementalEvents() {
        return incrementalEvents;
    }

    public long getDdlEvents() {
        return ddlEvents;
    }

    public long getInflightEvents() {
        return inflightEvents;
    }

    public int getShardSourceCount() {
        return shardSourceCount;
    }

    public Long getLastEventTsMs() {
        return lastEventTsMs;
    }

    public long getStartedAtMs() {
        return startedAtMs;
    }

    public Long getCommittedAtMs() {
        return committedAtMs;
    }

    public String getDetail() {
        return detail;
    }

    @Override
    public String toString() {
        return "MigrationProgress{"
                + "state=" + state
                + ", canCommit=" + canCommit
                + ", fullSyncComplete=" + fullSyncComplete
                + ", snapshotEvents=" + snapshotEvents
                + ", incrementalEvents=" + incrementalEvents
                + ", ddlEvents=" + ddlEvents
                + ", inflightEvents=" + inflightEvents
                + ", shardSourceCount=" + shardSourceCount
                + ", lastEventTsMs=" + lastEventTsMs
                + ", startedAtMs=" + startedAtMs
                + ", committedAtMs=" + committedAtMs
                + ", detail='" + detail + '\''
                + '}';
    }
}
