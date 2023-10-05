package com.github.lant.wal;

import java.util.UUID;

public interface Wal {
    long write(String key, String value);
    void commit(long walId);
}
