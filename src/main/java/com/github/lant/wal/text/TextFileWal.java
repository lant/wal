package com.github.lant.wal.text;

import com.github.lant.wal.Wal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalInt;

/**
 * This class stores the data in a series of WAL files.
 *
 * The files have a specific naming: XXX.wal
 *
 * In this specific implementation we are using a very basic Text file to store the text based contents. It is for
 * didactic purposes only as it's not an efficient implementation and does not provide proper error handling.
 *
 * In order to manage the size of the data stored in the WAL this system is splitting the files by size (currently
 * 1Mb). Once a file is full it transparently closes it and starts the next one.
 */
public class TextFileWal implements Wal {
    private static final int MAX_FILE_LENGTH = 1024 * 1024; // 1mb
    private static final boolean APPEND = true;
    private static final Logger logger = LoggerFactory.getLogger(TextFileWal.class);
    private final String rootDirectory;
    CleaningProcess cleaningProcess = new CleaningProcess();
    private FileOutputStream fileOutputStream = null;
    private File currentWalFile = null;
    private int currentWalFileIdx = 0;
    private WalFileUtils walFileUtils = null;

    public TextFileWal(String rootDirectory) throws IOException {
        this.rootDirectory = rootDirectory;
        Path baseDir = Path.of(rootDirectory);
        walFileUtils = new WalFileUtils(baseDir);
        if (Files.isDirectory(baseDir) && Files.isWritable(baseDir)) {
            // let's see if we already had some WAL files in the directory.
            OptionalInt previousIdx = walFileUtils.getPreviousIdx();
            if (previousIdx.isPresent()) {
                // if we have old files, check the size.
                if (Files.size(Path.of(baseDir.toString(), getWalFileName(previousIdx.getAsInt()))) < MAX_FILE_LENGTH) {
                    // File is not full yet, continue with this one
                    currentWalFileIdx = previousIdx.getAsInt();
                    logger.info("Found a previous wal file idx, we'll continue with it. Starting at: " + currentWalFileIdx);
                } else {
                    // File is big enough, let's go for a new one.
                    currentWalFileIdx = previousIdx.getAsInt() + 1;
                    logger.info("Found a previous wal file idx, too big to reuse. Starting at: " + currentWalFileIdx);
                }
            }
            try {
                // Open a stream writer to the specific file.
                String nextFile = getWalFileName(currentWalFileIdx);
                Path wal = Path.of(rootDirectory, nextFile);
                if (!Files.exists(wal)) {
                    currentWalFile = Files.createFile(wal).toFile();
                } else {
                    currentWalFile = wal.toFile();
                }
                this.fileOutputStream = new FileOutputStream(currentWalFile, APPEND);
            } catch (IOException e) {
                logger.error("Could not create WAL file", e);
                throw new RuntimeException(e);
            }
        }

        logger.info("Starting the wal file garbage collector");
        new Thread(cleaningProcess).start();
    }

    private String getWalFileName(int idx) {
        return String.format("%03d", idx) + ".wal";
    }

    @Override
    public long write(String key, String value) {
        // we are using microseconds as the key in order to know
        // the order of the data.
        long now = System.currentTimeMillis();
        // "serialise"
        String serialised = now + "-" + key + "-" + value + "\n";
        try {
            // write into the file.
            this.fileOutputStream.write(serialised.getBytes());
            // make sure that the data is actually persisted into disk.
            this.fileOutputStream.flush();

            // check size, and roll it if needed.
            if (Files.size(this.currentWalFile.toPath()) >= MAX_FILE_LENGTH) {
                // roll file
                currentWalFileIdx++;
                String nextFile = getWalFileName(currentWalFileIdx);
                logger.info("Rolling wal file to " + nextFile);
                this.currentWalFile = Path.of(rootDirectory, nextFile).toFile();
                this.fileOutputStream = new FileOutputStream(this.currentWalFile);
            }

        } catch (IOException e) {
            logger.error("Could not write into WAL file", e);
            return 0;
        }
        return now;
    }

    @Override
    public void commit(long walId) {
        cleaningProcess.setLatestIdx(walId);
    }

    @Override
    public void close() {
        cleaningProcess.close();
    }
}
