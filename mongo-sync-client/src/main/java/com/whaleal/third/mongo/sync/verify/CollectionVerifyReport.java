package com.whaleal.third.mongo.sync.verify;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 单集合比对结果。
 */
public final class CollectionVerifyReport {

    private final String sourceNs;
    private final String targetNs;
    private final long sourceCount;
    private final long targetCount;
    private final long missingOnTarget;
    private final long missingOnSource;
    private final long contentMismatch;
    private final long compared;
    private final List<String> samples;
    private final boolean passed;

    public CollectionVerifyReport(String sourceNs,
                                  String targetNs,
                                  long sourceCount,
                                  long targetCount,
                                  long missingOnTarget,
                                  long missingOnSource,
                                  long contentMismatch,
                                  long compared,
                                  List<String> samples) {
        this.sourceNs = sourceNs;
        this.targetNs = targetNs;
        this.sourceCount = sourceCount;
        this.targetCount = targetCount;
        this.missingOnTarget = missingOnTarget;
        this.missingOnSource = missingOnSource;
        this.contentMismatch = contentMismatch;
        this.compared = compared;
        this.samples = samples == null
                ? Collections.<String>emptyList()
                : Collections.unmodifiableList(new ArrayList<String>(samples));
        this.passed = sourceCount == targetCount
                && missingOnTarget == 0
                && missingOnSource == 0
                && contentMismatch == 0;
    }

    public String getSourceNs() {
        return sourceNs;
    }

    public String getTargetNs() {
        return targetNs;
    }

    public long getSourceCount() {
        return sourceCount;
    }

    public long getTargetCount() {
        return targetCount;
    }

    public long getMissingOnTarget() {
        return missingOnTarget;
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
        sb.append(" ").append(sourceNs).append(" -> ").append(targetNs);
        sb.append(" count=").append(sourceCount).append("/").append(targetCount);
        sb.append(" missingTarget=").append(missingOnTarget);
        sb.append(" missingSource=").append(missingOnSource);
        sb.append(" mismatch=").append(contentMismatch);
        sb.append(" compared=").append(compared);
        return sb.toString();
    }
}
