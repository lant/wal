package com.github.lant.wal.text;

import com.github.lant.wal.Wal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * This class stores the data in a WAL file.
 *
 * In this specific implementation we are using a very basic Text file to store the text based contents. It is for
 * didactic purposes only as it's not an efficient implementation and does not provide proper error handling.
 */
public class TextFileWal implements Wal {
    private static final int MAX_FILE_LINES = 10;
    private static final Logger logger = LoggerFactory.getLogger(TextFileWal.class);
    private int linesInFile;

    private FileOutputStream fileOutputStream = null;
    private File walInfo = null;

    public TextFileWal(String rootDirectory) {
        Path baseDir = Path.of(rootDirectory);
        if (Files.isDirectory(baseDir) && Files.isWritable(baseDir)) {
            File walFile = null;
            try {
                // look which wal file we need now
                walFile = Files.createFile(Path.of(rootDirectory, "data.wal")).toFile();
                walInfo = Files.createFile(Path.of(rootDirectory, "metadata.wal")).toFile();
                this.fileOutputStream = new FileOutputStream(walFile);
            } catch (IOException e) {
                logger.error("Could not create WAL file", e);
                throw new RuntimeException(e);
            }
        }

        // if metadata.wal already exists start the cleaning process.
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
            this.linesInFile++;

            if (this.linesInFile == MAX_FILE_LINES) {
                // open new file
            }
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
