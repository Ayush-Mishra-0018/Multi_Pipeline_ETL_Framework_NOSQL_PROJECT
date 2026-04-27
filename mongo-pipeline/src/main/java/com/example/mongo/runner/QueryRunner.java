package com.example.mongo.runner;

import com.example.mongo.queries.Query1_DailyTrafficQuery;
import com.example.mongo.queries.Query2_TopResources;
import com.mongodb.client.MongoDatabase;
import com.example.mongo.service.MongoConnection;
import com.example.mongo.queries.Query3_HourlyErrorAnalysis;

public class QueryRunner {

    public static void runQuery1() {

        try {
            // Get database from existing connection class
            MongoDatabase database = MongoConnection.getDatabase();

            // Run Query 3
            Query1_DailyTrafficQuery.run();

            System.out.println("Query 2 executed successfully.");

        } catch (Exception e) {
            System.out.println("Failed to execute Query 3");
            e.printStackTrace();

        } finally {
            MongoConnection.close();
        }
    }

    public static void runQuery2() {

        try {
            // Get database from existing connection class
            MongoDatabase database = MongoConnection.getDatabase();

            // Run Query 3
            Query2_TopResources.run();

            System.out.println("Query 2 executed successfully.");

        } catch (Exception e) {
            System.out.println("Failed to execute Query 3");
            e.printStackTrace();

        } finally {
            MongoConnection.close();
        }
    }

    public static void runQuery3() {

        try {
            System.out.println("\n========== QUERY 3 : Hourly Error Analysis ==========");
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