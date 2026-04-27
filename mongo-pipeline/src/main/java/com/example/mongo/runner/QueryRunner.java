package com.example.mongo.runner;

import com.mongodb.client.MongoDatabase;
import com.example.mongo.service.MongoConnection;
import com.example.mongo.queries.Query3_HourlyErrorAnalysis;

public class QueryRunner {

    public static void runQuery3() {

        try {
            // Get database from existing connection class
            MongoDatabase database = MongoConnection.getDatabase();

            // Run Query 3
            Query3_HourlyErrorAnalysis.run(database);

            System.out.println("Query 3 executed successfully.");

        } catch (Exception e) {
            System.out.println("Failed to execute Query 3");
            e.printStackTrace();

        } finally {
            MongoConnection.close();
        }
    }
}