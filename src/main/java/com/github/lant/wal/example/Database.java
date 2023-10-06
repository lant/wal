package com.github.lant.wal.example;

import com.github.lant.wal.text.TextFileWal;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Class that showcases how a system is using the WAL underneath in order
 * to add resiliency to its operations.
 */
public class Database {
    Map<String, String> data = new HashMap<>();
    TextFileWal wal = new TextFileWal("/tmp/wal/");

    public Database() throws IOException {
    }

    public void writeKeyValue(String key, String value) {
        // write the data to the wal storage
        long walId = wal.write(key, value);
        if (walId == 0L ) {
            throw new RuntimeException("Could not store data in the WAL file.");
        }

        // store the data in the DB storage engine
        data.put(key, value);

        // once the data is saved we can mark this data as not needed
        // in the wal storage.
        wal.commit(walId);
    }

    public void close() throws IOException {
        wal.close();
    }
}
