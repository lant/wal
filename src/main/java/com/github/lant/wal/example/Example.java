package com.github.lant.wal.example;

public class Example {
    public static void main(String[] args) {
        Database db = new Database();
        db.writeKeyValue("key", "value");
        db.writeKeyValue("key2", "value2");
    }
}
