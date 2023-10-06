package com.github.lant.wal;

import java.io.Closeable;
import java.util.UUID;

public interface Wal extends Closeable {
    long write(String key, String value);
    void commit(long walId);
}
