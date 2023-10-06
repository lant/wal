package com.github.lant.wal.example;

import java.io.IOException;

public class Example {
    public static void main(String[] args) throws IOException {
        Database db = new Database();
        for (int i = 0; i < 1000000; i++) {
            db.writeKeyValue("key2", "value2");
        }
        db.close();
    }
}
