package com.example.mapReduce.runner;

import com.example.mapReduce.queries.Query1_DailyTraffic_Global;
import com.example.mapReduce.queries.Query2_TopResources;
import com.example.mapReduce.queries.Query3_HourlyErrorAnalysis;
import com.example.postgres.service.PostgresSchemaInitializer;


import java.util.List;

public class QueryRunner {

    public static long runQueries(
            List<Integer> queries
    ) {

        long startTime =
                System.currentTimeMillis();

        PostgresSchemaInitializer.initialize( // this calls init
                "mapreduce"
        );

        for (int query : queries) {

            switch (query) {

                case 1:
                    runQuery1(); // write their output in pg
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
        // global insert of postgres

        long endTime =
                System.currentTimeMillis();

        return endTime - startTime;
    }

    public static void runQuery1() { // this calls insert

        try {

            Query1_DailyTraffic_Global.run();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void runQuery2() {

        try {
            Query2_TopResources.run();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void runQuery3() {

        try {
            Query3_HourlyErrorAnalysis.run();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}