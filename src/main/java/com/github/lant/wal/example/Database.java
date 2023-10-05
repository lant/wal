package com.github.lant.wal.example;

import com.github.lant.wal.Wal;
import com.github.lant.wal.text.TextFileWal;

import java.util.HashMap;
import java.util.Map;

public class Database {
    Map<String, String> data = new HashMap<>();
    Wal wal = new TextFileWal("/tmp/");

    public void writeKeyValue(String key, String value) {
        long walId = wal.write(key, value);
        if (walId == 0L ) {
            throw new RuntimeException("Could not store data in the WAL file.");
        }
        data.put(key, value);
        wal.commit(walId);
    }
}
