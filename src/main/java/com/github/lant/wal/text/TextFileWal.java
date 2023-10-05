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
import java.util.stream.IntStream;

/**
 * This class stores the data in a WAL file.
 * In this specific implementation we are using a very basic Text file to store the text based contents. It is for
 * didactic purposes only as it's not an efficient implementation and does not provide proper error handling.
 */
public class TextFileWal implements Wal {
    private static final int MAX_FILE_LENGTH = 1024 * 1024; // 1mb
    private static final boolean APPEND = true;
    private static final Logger logger = LoggerFactory.getLogger(TextFileWal.class);

    private FileOutputStream fileOutputStream = null;

    public TextFileWal(String rootDirectory) throws IOException {
        Path baseDir = Path.of(rootDirectory);
        if (Files.isDirectory(baseDir) && Files.isWritable(baseDir)) {
            // let's see if we already had some WAL files in there.
            OptionalInt previousIdx = getPreviousIdx(baseDir);
            int currentWalFileIdx = 0;
            if (previousIdx.isPresent()) {
                // check the size.
                if (Files.size(Path.of(baseDir.toString(), getWalFileName(previousIdx.getAsInt()))) < MAX_FILE_LENGTH) {
                    // continue with this one
                    currentWalFileIdx = previousIdx.getAsInt();
                    logger.info("Found a previous wal file idx, we'll continue with it. Starting at: " + currentWalFileIdx);
                } else {
                    // let's go for a new one.
                    currentWalFileIdx = previousIdx.getAsInt() + 1;
                    logger.info("Found a previous wal file idx, too big to reuse. Starting at: " + currentWalFileIdx);
                }
            }
            try {
                // look which wal file we need now
                String nextFile = getWalFileName(currentWalFileIdx);
                Path wal = Path.of(rootDirectory, nextFile);
                File walFile = null;
                if (!Files.exists(wal)) {
                     walFile = Files.createFile(wal).toFile();
                } else {
                    walFile = wal.toFile();
                }
                this.fileOutputStream = new FileOutputStream(walFile, APPEND);
            } catch (IOException e) {
                logger.error("Could not create WAL file", e);
                throw new RuntimeException(e);
            }
        }

    }

    private String getWalFileName(int idx) {
        return String.format("%03d", idx) + ".wal";
    }

    public OptionalInt getPreviousIdx(Path baseDir) throws IOException {
        return Files.list(baseDir)
                .filter(path -> path.toString().endsWith(".wal"))
                .map(path -> path.getFileName().toString().split("\\.")[0])
                .flatMapToInt(name -> IntStream.of(Integer.parseInt(name))).max();
    }

    @Override
    public long write(String key, String value) {

        // "serialise"
        long now = System.currentTimeMillis();
        String serialised = now + "-" + key + "-" + value + "\n";
        try {
            // write into the file.
            this.fileOutputStream.write(serialised.getBytes());
            // make sure that the data is actually persisted into disk.
            this.fileOutputStream.flush();

        } catch (IOException e) {
            logger.error("Could not write into WAL file", e);
            return 0;
        }
        return now;
    }

    @Override
    public void commit(long walId) {
        // save the walId
        // start the cleaning process
    }
}
