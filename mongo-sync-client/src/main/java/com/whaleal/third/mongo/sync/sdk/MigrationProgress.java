package com.whaleal.third.mongo.sync.sdk;

/**
 * 迁移进度快照。
 */
public final class MigrationProgress {

    private final String namespace;
    private final String phase;
    private final MigrationState state;
    private final boolean canCommit;
    private final boolean fullSyncComplete;
    private final long estimatedTotalDocuments;
    private final long snapshotEvents;
    private final long incrementalEvents;
    private final long ddlEvents;
    private final long inflightEvents;
    private final int shardSourceCount;
    private final Long lastEventTsMs;
    private final long startedAtMs;
    private final Long committedAtMs;
    private final long elapsedMs;
    private final String detail;

    public MigrationProgress(String namespace,
                             String phase,
                             MigrationState state,
                             boolean canCommit,
                             boolean fullSyncComplete,
                             long estimatedTotalDocuments,
                             long snapshotEvents,
                             long incrementalEvents,
                             long ddlEvents,
                             long inflightEvents,
                             int shardSourceCount,
                             Long lastEventTsMs,
                             long startedAtMs,
                             Long committedAtMs,
                             long elapsedMs,
                             String detail) {
        this.namespace = namespace;
        this.phase = phase;
        this.state = state;
        this.canCommit = canCommit;
        this.fullSyncComplete = fullSyncComplete;
        this.estimatedTotalDocuments = estimatedTotalDocuments;
        this.snapshotEvents = snapshotEvents;
        this.incrementalEvents = incrementalEvents;
        this.ddlEvents = ddlEvents;
        this.inflightEvents = inflightEvents;
        this.shardSourceCount = shardSourceCount;
        this.lastEventTsMs = lastEventTsMs;
        this.startedAtMs = startedAtMs;
        this.committedAtMs = committedAtMs;
        this.elapsedMs = elapsedMs;
        this.detail = detail;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getPhase() {
        return phase;
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

    public long getEstimatedTotalDocuments() {
        return estimatedTotalDocuments;
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

    public long getElapsedMs() {
        return elapsedMs;
    }

    public String getDetail() {
        return detail;
    }

    public long getCopiedDocuments() {
        return snapshotEvents;
    }

    public int getFullSyncPercent() {
        if (estimatedTotalDocuments <= 0) {
            return fullSyncComplete ? 100 : 0;
        }
        long percent = (snapshotEvents * 100L) / estimatedTotalDocuments;
        if (fullSyncComplete && percent < 100L) {
            return 100;
        }
        return (int) Math.max(0L, Math.min(100L, percent));
    }

    @Override
    public String toString() {
        return "MigrationProgress{"
                + "namespace='" + namespace + '\''
                + ", phase='" + phase + '\''
                + ", state=" + state
                + ", canCommit=" + canCommit
                + ", fullSyncComplete=" + fullSyncComplete
                + ", estimatedTotalDocuments=" + estimatedTotalDocuments
                + ", snapshotEvents=" + snapshotEvents
                + ", incrementalEvents=" + incrementalEvents
                + ", ddlEvents=" + ddlEvents
                + ", inflightEvents=" + inflightEvents
                + ", shardSourceCount=" + shardSourceCount
                + ", lastEventTsMs=" + lastEventTsMs
                + ", startedAtMs=" + startedAtMs
                + ", committedAtMs=" + committedAtMs
                + ", elapsedMs=" + elapsedMs
                + ", detail='" + detail + '\''
                + '}';
    }
}
