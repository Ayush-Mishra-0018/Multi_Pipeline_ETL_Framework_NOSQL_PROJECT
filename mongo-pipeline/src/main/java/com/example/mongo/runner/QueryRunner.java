package com.example.mongo.runner;

import com.example.mongo.queries.Query1_DailyTraffic_Global;
import com.example.mongo.queries.Query2_TopResources;
import com.example.mongo.queries.Query3_HourlyErrorAnalysis;
import com.example.mongo.service.MongoConnection;
import com.example.postgres.service.PostgresSchemaInitializer;
import com.mongodb.client.MongoDatabase;

public class QueryRunner {

    public static void runQueries() {

        PostgresSchemaInitializer.initialize();

        runQuery1();
        runQuery2();
        runQuery3();
    }

    public static void runQuery1() {

        try {
            MongoDatabase database =
                    MongoConnection.getDatabase();

            Query1_DailyTraffic_Global.run(database);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void runQuery2() {

        try {
            MongoDatabase database =
                    MongoConnection.getDatabase();

            Query2_TopResources.run(database);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void runQuery3() {

        try {
            MongoDatabase database =
                    MongoConnection.getDatabase();

            Query3_HourlyErrorAnalysis.run(database);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}