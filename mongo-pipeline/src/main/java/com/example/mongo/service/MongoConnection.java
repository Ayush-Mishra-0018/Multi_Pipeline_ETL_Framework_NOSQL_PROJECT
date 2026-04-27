package com.example.mongo.service;

import com.example.config.ConfigReader;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

public final class MongoConnection {

    private static final String URI =
            ConfigReader.get("mongo.uri");

    private static final String DATABASE_NAME =
            ConfigReader.get("mongo.database");

    private static final MongoClient CLIENT =
            MongoClients.create(URI);

    private MongoConnection() {
    }

    public static MongoDatabase getDatabase() {
        return CLIENT.getDatabase(DATABASE_NAME);
    }

    public static void close() {
        CLIENT.close();
    }
}