package com.github.lant.wal.example;

import com.github.lant.wal.Wal;
import com.github.lant.wal.text.CleaningProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

public class DatabaseProcessor implements Runnable, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseProcessor.class);

    private final Random rd = new Random();
    private final Wal wal;
    private final LinkedBlockingQueue<Data> queue;
    private boolean run = true;

    public DatabaseProcessor(Wal wal, LinkedBlockingQueue<Data> queue) {
        this.wal = wal;
        this.queue = queue;
    }
    @Override
    public void run() {
       while (run) {
           try {
               Data data = queue.take();
               Thread.sleep(rd.nextInt(100, 500));
               logger.info("Processing " + data);
               wal.commit(data.getIdx());
           } catch (InterruptedException e) {
               throw new RuntimeException(e);
           }
       }
    }

    @Override
    public void close() {
        this.run = false;
    }
}
