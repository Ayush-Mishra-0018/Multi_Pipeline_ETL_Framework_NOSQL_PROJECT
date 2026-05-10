package com.example.mongo.runner;

import com.example.mongo.queries.Query1_DailyTraffic_Global;
import com.example.mongo.queries.Query2_TopResources;
import com.example.mongo.queries.Query3_HourlyErrorAnalysis;
import com.example.mongo.service.MongoConnection;
import com.example.postgres.service.PostgresSchemaInitializer;
import com.mongodb.client.MongoDatabase;

import java.util.List;

public class QueryRunner {

    public static long runQueries(
            List<Integer> queries
    ) {

        long startTime =
                System.currentTimeMillis();

        PostgresSchemaInitializer.initialize(
                "mongodb"
        );

        for (int query : queries) {

            switch (query) {

                case 1:
                    runQuery1();
                    break;

                case 2:
                    runQuery2();
                    break;

                case 3:
                    runQuery3();
                    break;

                default:
                    System.out.println(
                            "Invalid query number: " + query
                    );
            }
        }

        long endTime =
                System.currentTimeMillis();

        return endTime - startTime;
    }

    public static void runQuery1() {

        try {

            MongoDatabase database =
                    MongoConnection.getDatabase();

            Query1_DailyTraffic_Global.run(
                    database
            );

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void runQuery2() {

        try {

            MongoDatabase database =
                    MongoConnection.getDatabase();

            Query2_TopResources.run(
                    database
            );

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void runQuery3() {

        try {

            MongoDatabase database =
                    MongoConnection.getDatabase();

            Query3_HourlyErrorAnalysis.run(
                    database
            );

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}