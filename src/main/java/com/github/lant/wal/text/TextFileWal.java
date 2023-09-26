package com.github.lant.wal.text;

import com.github.lant.wal.Wal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class TextFileWal implements Wal {
    private static Logger logger = LoggerFactory.getLogger(TextFileWal.class);

    private FileOutputStream fileOutputStream = null;

    public TextFileWal(String rootDirectory) throws IOException {
        Path baseDir = Path.of(rootDirectory);
        if (Files.isDirectory(baseDir) && Files.isWritable(baseDir)) {
            File walFile = Files.createFile(Path.of(rootDirectory, "file.wal")).toFile();
            this.fileOutputStream = new FileOutputStream(walFile);
        }
    }
    @Override
    public boolean write(String key, String value) {
        // "serialise"
        String serialised = key + "-" + value + "\n";
        try {
            // write into the file.
            this.fileOutputStream.write(serialised.getBytes());
            // make sure that the data is actually persisted into disk.
            this.fileOutputStream.flush();
        } catch (IOException e) {
            logger.error("Could not write into WAL file", e);
            return false;
        }
        return true;
    }
}
