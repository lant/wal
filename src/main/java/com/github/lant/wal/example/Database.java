package com.github.lant.wal.example;

import com.github.lant.wal.text.TextFileWal;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

public class Database {
    private final TextFileWal wal = new TextFileWal("/tmp/wal/");
    private final LinkedBlockingQueue<Data> queue = new LinkedBlockingQueue<>();
    private final DatabaseProcessor processor;

    public Database() throws IOException {
        processor = new DatabaseProcessor(wal, queue);
        new Thread(processor).start();
    }

    public void writeKeyValue(String key, String value) {
        long walId = wal.write(key, value);
        if (walId == 0L ) {
            throw new RuntimeException("Could not store data in the WAL file.");
        }
        queue.add(new Data(key, value, walId));
    }

    public void close() throws IOException {
        wal.close();
        processor.close();
    }
}
