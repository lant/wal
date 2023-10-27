package com.github.lant.wal.example;

import com.github.lant.wal.text.TextFileWal;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Database {
    private final TextFileWal wal = new TextFileWal("/tmp/wal/");
    private final LinkedBlockingQueue<Data> queue = new LinkedBlockingQueue<>();
    private final DatabaseProcessor processor;
    private static final Logger logger = LoggerFactory.getLogger(Database.class);

    public Database() throws IOException {
        processor = new DatabaseProcessor(wal, queue);
        new Thread(processor).start();

        // reprocess backlog, if any
        try {
            wal.getBacklog().forEach(queue::add);
        } catch (Exception e) {
            logger.info("No files to be reexecuted."); 
        }
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
