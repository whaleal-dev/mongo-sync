package com.whaleal.third.mongo.sync.sdk;

import org.bson.Document;

/**
 * 迁移进度快照。
 */
public final class MigrationProgress {

    private final String namespace;
    private final String phase;
    private final String topology;
    private final String captureMode;
    private final String syncMode;
    private final MigrationState state;
    private final boolean canCommit;
    private final boolean fullSyncComplete;
    private final long estimatedTotalDocuments;
    private final long snapshotEvents;
    private final long incrementalEvents;
    private final long ddlEvents;
    private final long inflightEvents;
    private final Long lastEventTsMs;
    private final long startedAtMs;
    private final Long committedAtMs;
    private final long elapsedMs;
    private final Long lagMs;
    private final long namespaceCount;
    private final String detail;
    private final String commitReadiness;

    public MigrationProgress(String namespace,
                             String phase,
                             String topology,
                             String captureMode,
                             String syncMode,
                             MigrationState state,
                             boolean canCommit,
                             boolean fullSyncComplete,
                             long estimatedTotalDocuments,
                             long snapshotEvents,
                             long incrementalEvents,
                             long ddlEvents,
                             long inflightEvents,
                             Long lastEventTsMs,
                             long startedAtMs,
                             Long committedAtMs,
                             long elapsedMs,
                             Long lagMs,
                             long namespaceCount,
                             String detail,
                             String commitReadiness) {
        this.namespace = namespace;
        this.phase = phase;
        this.topology = topology;
        this.captureMode = captureMode;
        this.syncMode = syncMode;
        this.state = state;
        this.canCommit = canCommit;
        this.fullSyncComplete = fullSyncComplete;
        this.estimatedTotalDocuments = estimatedTotalDocuments;
        this.snapshotEvents = snapshotEvents;
        this.incrementalEvents = incrementalEvents;
        this.ddlEvents = ddlEvents;
        this.inflightEvents = inflightEvents;
        this.lastEventTsMs = lastEventTsMs;
        this.startedAtMs = startedAtMs;
        this.committedAtMs = committedAtMs;
        this.elapsedMs = elapsedMs;
        this.lagMs = lagMs;
        this.namespaceCount = namespaceCount;
        this.detail = detail;
        this.commitReadiness = commitReadiness;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getPhase() {
        return phase;
    }

    public String getTopology() {
        return topology;
    }

    public String getCaptureMode() {
        return captureMode;
    }

    public String getSyncMode() {
        return syncMode;
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

    public Long getLagMs() {
        return lagMs;
    }

    public long getNamespaceCount() {
        return namespaceCount;
    }

    public String getDetail() {
        return detail;
    }

    public String getCommitReadiness() {
        return commitReadiness;
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

    public long getRemainingDocumentsEstimate() {
        long remain = estimatedTotalDocuments - snapshotEvents;
        return Math.max(0L, remain);
    }

    public Document toDocument() {
        Document doc = new Document();
        doc.put("namespace", namespace);
        doc.put("phase", phase);
        doc.put("topology", topology);
        doc.put("captureMode", captureMode);
        doc.put("syncMode", syncMode);
        doc.put("state", state == null ? null : state.name());
        doc.put("canCommit", canCommit);
        doc.put("fullSyncComplete", fullSyncComplete);
        doc.put("estimatedTotalDocuments", estimatedTotalDocuments);
        doc.put("copiedDocuments", getCopiedDocuments());
        doc.put("remainingDocumentsEstimate", getRemainingDocumentsEstimate());
        doc.put("fullSyncPercent", getFullSyncPercent());
        doc.put("snapshotEvents", snapshotEvents);
        doc.put("incrementalEvents", incrementalEvents);
        doc.put("ddlEvents", ddlEvents);
        doc.put("inflightEvents", inflightEvents);
        doc.put("lastEventTsMs", lastEventTsMs);
        doc.put("startedAtMs", startedAtMs);
        doc.put("committedAtMs", committedAtMs);
        doc.put("elapsedMs", elapsedMs);
        doc.put("lagMs", lagMs);
        doc.put("namespaceCount", namespaceCount);
        doc.put("detail", detail);
        doc.put("commitReadiness", commitReadiness);
        return doc;
    }

    @Override
    public String toString() {
        return "MigrationProgress{"
                + "namespace='" + namespace + '\''
                + ", phase='" + phase + '\''
                + ", topology='" + topology + '\''
                + ", captureMode='" + captureMode + '\''
                + ", syncMode='" + syncMode + '\''
                + ", state=" + state
                + ", canCommit=" + canCommit
                + ", fullSyncComplete=" + fullSyncComplete
                + ", estimatedTotalDocuments=" + estimatedTotalDocuments
                + ", copiedDocuments=" + getCopiedDocuments()
                + ", remainingDocumentsEstimate=" + getRemainingDocumentsEstimate()
                + ", fullSyncPercent=" + getFullSyncPercent()
                + ", snapshotEvents=" + snapshotEvents
                + ", incrementalEvents=" + incrementalEvents
                + ", ddlEvents=" + ddlEvents
                + ", inflightEvents=" + inflightEvents
                + ", lastEventTsMs=" + lastEventTsMs
                + ", startedAtMs=" + startedAtMs
                + ", committedAtMs=" + committedAtMs
                + ", elapsedMs=" + elapsedMs
                + ", lagMs=" + lagMs
                + ", namespaceCount=" + namespaceCount
                + ", commitReadiness='" + commitReadiness + '\''
                + ", detail='" + detail + '\''
                + '}';
    }
}
