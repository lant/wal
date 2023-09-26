package com.github.lant.wal;

public interface Wal {
    boolean write(String key, String value);
}
