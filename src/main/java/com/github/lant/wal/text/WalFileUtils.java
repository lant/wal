package com.github.lant.wal.text;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalInt;
import java.util.stream.IntStream;

public class WalFileUtils {
    private final Path baseDir;

    public WalFileUtils(Path baseDir) {
        this.baseDir = baseDir;
    }

    public OptionalInt getPreviousIdx() throws IOException {
        return Files.list(baseDir)
                .filter(path -> path.toString().endsWith(".wal"))
                .map(path -> path.getFileName().toString().split("\\.")[0])
                .flatMapToInt(name -> IntStream.of(Integer.parseInt(name))).max();
    }

}
