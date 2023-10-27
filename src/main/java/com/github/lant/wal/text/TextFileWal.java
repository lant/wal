package com.github.lant.wal.text;

import com.github.lant.wal.Wal;
import com.github.lant.wal.example.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Stream;

/**
 * This class stores the data in a WAL file.
 * In this specific implementation we are using a very basic Text file to store the text based contents. It is for
 * didactic purposes only as it's not an efficient implementation and does not provide proper error handling.
 */
public class TextFileWal implements Wal {
    private static final int MAX_FILE_LENGTH = 1024 * 1024; // 1mb
    private static final boolean APPEND = true;
    private static final boolean NO_APPEND = false;
    private static final Logger logger = LoggerFactory.getLogger(TextFileWal.class);
    private final String rootDirectory;
    CleaningProcess cleaningProcess = new CleaningProcess();
    private FileOutputStream fileOutputStream = null;
    private File currentWalFile = null;
    private final File commitLog = new File("/tmp/wal/commit.log");
    private int currentWalFileIdx = 0;

    public TextFileWal(String rootDirectory) throws IOException {
        this.rootDirectory = rootDirectory;
        Path baseDir = Path.of(rootDirectory);
        WalFileUtils walFileUtils = new WalFileUtils(baseDir);
        if (Files.isDirectory(baseDir) && Files.isWritable(baseDir)) {
            // let's see if we already had some WAL files in there.
            OptionalInt previousIdx = walFileUtils.getPreviousIdx();
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
        if (!commitLog.exists()) {
            commitLog.createNewFile();
        }

        logger.info("Starting the wal file garbage collector");
        new Thread(cleaningProcess).start();
    }

    private String getWalFileName(int idx) {
        return String.format("%03d", idx) + ".wal";
    }


    @Override
    public long write(String key, String value) {
        // "serialise"
        long now = System.nanoTime();
        String serialised = now + "-" + key + "-" + value + "\n";
        try {
            // write into the file.
            this.fileOutputStream.write(serialised.getBytes());
            // make sure that the data is actually persisted into disk.
            this.fileOutputStream.flush();

            // check size
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
        // store the stuff safely in file.
        try {
            try (FileOutputStream commitLog = new FileOutputStream("/tmp/wal/commit.log", NO_APPEND)) {
                commitLog.write(Long.toString(walId).getBytes());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        cleaningProcess.setLatestIdx(walId);
    }


    @Override
    public Stream<Data> getBacklog() {
        // get commit log
        try {
            BufferedReader commitLogReader = new BufferedReader(new FileReader(commitLog));
            String line = commitLogReader.readLine();
            commitLogReader.close();
            long lastCommitedIdx;
            if (line != null) {
                lastCommitedIdx = Long.parseLong(line);
            } else {
                lastCommitedIdx = 0L;
            }
            logger.info("Latest commit IDX = " + lastCommitedIdx);

            // go through wal files to see if we find that commit log / something newer
            List<Path> walFiles = Files.list(Path.of("/tmp/wal/"))
                    .sorted().filter(file -> !file.getFileName().toString().matches("commit.log")).toList();

            return walFiles.stream()
                    .flatMap(this::fileToDataStream)
                    .filter(data -> filterAlreadyCommitedIndexes(data, lastCommitedIdx));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private boolean filterAlreadyCommitedIndexes(Data dataRecord, long lastCommitedIdx) {
        boolean allowed = dataRecord.getIdx() > lastCommitedIdx;
        if (!allowed) {
            logger.info("Filtering IDX: " + dataRecord.getIdx() + " as it's lower than the last committed IDX("+lastCommitedIdx+")");
        }
        return allowed;
    }

    private Stream<Data> fileToDataStream(Path file) {
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file.toString()));
            return bufferedReader.lines().map(line -> {
                String[] parts = line.split("-");
                long idx = Long.parseLong(parts[0]);
                String key = parts[1];
                String value = parts[2];
                return new Data(key, value, idx);
            });

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        cleaningProcess.close();
    }
}
