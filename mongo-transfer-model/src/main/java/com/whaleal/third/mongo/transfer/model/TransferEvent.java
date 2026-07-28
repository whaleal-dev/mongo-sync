package com.whaleal.third.mongo.transfer.model;

import java.util.Map;

/**
 * 通用数据传输事件（捕获方式无关）。
 * <p>
 * Source（Oplog / ChangeStream）产出本模型；Sink 只识别本模型写入。
 * <ul>
 *   <li>{@code c}/{@code r} — after = 全量文档</li>
 *   <li>{@code u} — after = 全量文档或 {@code $set/$unset}；before = preImage 或 documentKey</li>
 *   <li>{@code d} — before = preImage 或 documentKey</li>
 * </ul>
 */
public class TransferEvent {

    private Map<String, Object> before;
    private String op;
    private Map<String, Object> after;
    private TransferSource source;
    private Long tsMs;

    public TransferEvent() {
    }

    public static Builder builder() {
        return new Builder();
    }

    public Map<String, Object> getBefore() {
        return before;
    }

    public void setBefore(Map<String, Object> before) {
        this.before = before;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public Map<String, Object> getAfter() {
        return after;
    }

    public void setAfter(Map<String, Object> after) {
        this.after = after;
    }

    public TransferSource getSource() {
        return source;
    }

    public void setSource(TransferSource source) {
        this.source = source;
    }

    public Long getTsMs() {
        return tsMs;
    }

    public void setTsMs(Long tsMs) {
        this.tsMs = tsMs;
    }

    public static class Builder {
        private Map<String, Object> before;
        private String op;
        private Map<String, Object> after;
        private TransferSource source;
        private Long tsMs;

        public Builder before(Map<String, Object> before) {
            this.before = before;
            return this;
        }

        public Builder op(String op) {
            this.op = op;
            return this;
        }

        public Builder after(Map<String, Object> after) {
            this.after = after;
            return this;
        }

        public Builder source(TransferSource source) {
            this.source = source;
            return this;
        }

        public Builder tsMs(Long tsMs) {
            this.tsMs = tsMs;
            return this;
        }

        public TransferEvent build() {
            TransferEvent event = new TransferEvent();
            event.before = this.before;
            event.op = this.op;
            event.after = this.after;
            event.source = this.source;
            event.tsMs = this.tsMs;
            return event;
        }
    }
}
