package com.github.lant.wal.example;

import java.io.IOException;

public class Example {
    public static void main(String[] args) throws IOException {
        Database db = new Database();
        for (int i = 0; i < 1000000; i++) {
            db.writeKeyValue("key"+i, "value"+i);
            if (i % 1000 == 0) {
                System.out.println("Inserting data: " + i);
            }
        }
        db.close();
    }
}
