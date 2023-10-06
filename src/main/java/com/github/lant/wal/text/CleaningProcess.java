package com.github.lant.wal.text;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class takes care of deleting old and unnecessary
 * WAL files in the storage system.
 *
 * This class is a separate thread, as we don't want to block the main thread.
 *
 * The main method performing cleanups periodically (every 10s) for convenience. The cleanup
 * process should be triggered by other factors as well, but for this example this is enough.
 */
public class CleaningProcess implements Runnable, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(CleaningProcess.class);
    private boolean run = true;
    private long latestIdx;

    @Override
    public void run() {
        while(run) {
            try {
                Thread.sleep(10 * 1000); // 10 s
                cleanup(latestIdx);
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    private void cleanup(long latestIdx) throws IOException {
        logger.info("Starting clean up process");
        // list files in order
        List<Path> walFiles = Files.list(Path.of("/tmp/wal/"))
                .sorted().collect(Collectors.toList());
        // exclude the current idx, we don't want to delete the file that's being currently used.
        walFiles.remove(walFiles.size() - 1);

        // for each file: check the last key and if the key is smaller that the last committed
        // key by the client at the time of starting the cleanup we can delete the full file as
        // we know for sure that the rest of the keys are older than the last key.
        for (Path walFile : walFiles) {
            String lastRecord = "";
            String currentLine = "";
            try (BufferedReader fileReader =
                         new BufferedReader(new FileReader(walFile.toFile()))) {
                while ((currentLine = fileReader.readLine()) != null) {
                    lastRecord = currentLine;
                }
            }
            long lastRecordIdx = Long.parseLong(lastRecord.split("-")[0]);
            if (lastRecordIdx < latestIdx) {
                logger.info("The last idx contained in " + walFile.getFileName() +" is smaller that " +
                        "the commit record ["+lastRecordIdx+"]. Deleting WAL file.");
                Files.deleteIfExists(walFile);
            }
        }
    }

    @Override
    public void close() {
        this.run = false;
    }

    public void setLatestIdx(long latestIdx) {
        this.latestIdx = latestIdx;
    }
}
