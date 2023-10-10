package com.github.lant.wal.example;

public class Data {

    private final String key;
    private final String value;
    private final Long idx;
    public Data(String key, String value, long idx) {
        this.key = key;
        this.value = value;
        this.idx = idx;
    }

    public Long getIdx() {
        return idx;
    }

    @Override
    public String toString() {
        return "Data{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", idx=" + idx +
                '}';
    }
}
