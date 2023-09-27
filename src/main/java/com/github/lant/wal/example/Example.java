package com.github.lant.wal.example;

import com.github.lant.wal.text.TextFileWal;
import com.github.lant.wal.Wal;

import java.io.IOException;

public class Example {
    public static void main(String[] args) throws IOException {
        Database db = new Database();
        db.writeKeyValue("key", "value");
        db.writeKeyValue("key2", "value2");
    }
}
