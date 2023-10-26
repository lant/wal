package com.github.lant.wal;

import com.github.lant.wal.example.Data;

import java.io.Closeable;
import java.util.stream.Stream;

public interface Wal extends Closeable {
    long write(String key, String value);
    void commit(long walId);

    Stream<Data> getBacklog();
}
