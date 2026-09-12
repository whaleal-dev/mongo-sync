package com.whaleal.third.mongo.sync.verify;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 单集合比对结果。
 */
public final class CollectionVerifyReport {

    private final String sourceNs;
    private final String sinkNs;
    private final long sourceCount;
    private final long sinkCount;
    private final long missingOnSink;
    private final long missingOnSource;
    private final long contentMismatch;
    private final long compared;
    private final List<String> samples;
    private final boolean passed;

    public CollectionVerifyReport(String sourceNs,
                                  String sinkNs,
                                  long sourceCount,
                                  long sinkCount,
                                  long missingOnSink,
                                  long missingOnSource,
                                  long contentMismatch,
                                  long compared,
                                  List<String> samples) {
        this.sourceNs = sourceNs;
        this.sinkNs = sinkNs;
        this.sourceCount = sourceCount;
        this.sinkCount = sinkCount;
        this.missingOnSink = missingOnSink;
        this.missingOnSource = missingOnSource;
        this.contentMismatch = contentMismatch;
        this.compared = compared;
        this.samples = samples == null
                ? Collections.<String>emptyList()
                : Collections.unmodifiableList(new ArrayList<String>(samples));
        this.passed = sourceCount == sinkCount
                && missingOnSink == 0
                && missingOnSource == 0
                && contentMismatch == 0;
    }

    public String getSourceNs() {
        return sourceNs;
    }

    public String getSinkNs() {
        return sinkNs;
    }

    public long getSourceCount() {
        return sourceCount;
    }

    public long getSinkCount() {
        return sinkCount;
    }

    public long getMissingOnSink() {
        return missingOnSink;
    }

    public long getMissingOnSource() {
        return missingOnSource;
    }

    public long getContentMismatch() {
        return contentMismatch;
    }

    public long getCompared() {
        return compared;
    }

    public List<String> getSamples() {
        return samples;
    }

    public boolean isPassed() {
        return passed;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(passed ? "PASS" : "FAIL");
        sb.append(" ").append(sourceNs).append(" -> ").append(sinkNs);
        sb.append(" count=").append(sourceCount).append("/").append(sinkCount);
        sb.append(" missingSink=").append(missingOnSink);
        sb.append(" missingSource=").append(missingOnSource);
        sb.append(" mismatch=").append(contentMismatch);
        sb.append(" compared=").append(compared);
        return sb.toString();
    }
}
