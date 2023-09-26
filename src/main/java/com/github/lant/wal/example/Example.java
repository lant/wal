package com.github.lant.wal.example;

import com.github.lant.wal.text.TextFileWal;
import com.github.lant.wal.Wal;

import java.io.IOException;

public class Example {
    public static void main(String[] args) throws IOException {
        Wal wal = new TextFileWal("/tmp/");
        wal.write("key", "value");
        wal.write("key2", "value2");
        wal.write("key3", "value3");

    }
}
