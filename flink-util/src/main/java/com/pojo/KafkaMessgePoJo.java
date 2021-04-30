package com.pojo;

public class KafkaMessgePoJo {
    public String topic;
    public String msg;
    public String rowtime;
    public String uid;
    public Long ts;
    public Long offset;

    public KafkaMessgePoJo(String topic,
                           Long offset,
                           Long ts,
                           String msg,
                           String rowtime,
                           String uid) {
        this.topic = topic;
        this.msg = msg;
        this.rowtime = rowtime;
        this.uid = uid;
        this.ts = ts;
        this.offset = offset;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getRowtime() {
        return rowtime;
    }

    public void setRowtime(String rowtime) {
        this.rowtime = rowtime;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "KafkaMessgePoJo{" +
                "topic='" + topic + '\'' +
                ", msg='" + msg + '\'' +
                ", rowtime='" + rowtime + '\'' +
                ", uid='" + uid + '\'' +
                ", ts=" + ts +
                ", offset=" + offset +
                '}';
    }

    public KafkaMessgePoJo() {
    }
}
