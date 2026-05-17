package com.example.pig.runner;

import com.example.pig.queries.Query1_DailyTraffic_Global;
import com.example.pig.queries.Query2_TopResources;
import com.example.pig.queries.Query3_HourlyErrorAnalysis;

import java.util.List;

public class PigQueryRunner {

    public static long runQueries(List<Integer> queries) {
        long startTime = System.currentTimeMillis();

        if (queries.contains(1)) {
            System.out.println("\nRunning Query 1: Daily Traffic Global (Pig)...");
            long start = System.currentTimeMillis();
            Query1_DailyTraffic_Global.run();
            System.out.println("Query 1 completed in " + (System.currentTimeMillis() - start) + " ms");
        }

        if (queries.contains(2)) {
            System.out.println("\nRunning Query 2: Top Resources (Pig)...");
            long start = System.currentTimeMillis();
            Query2_TopResources.run();
            System.out.println("Query 2 completed in " + (System.currentTimeMillis() - start) + " ms");
        }

        if (queries.contains(3)) {
            System.out.println("\nRunning Query 3: Hourly Error Analysis (Pig)...");
            long start = System.currentTimeMillis();
            Query3_HourlyErrorAnalysis.run();
            System.out.println("Query 3 completed in " + (System.currentTimeMillis() - start) + " ms");
        }

        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }

    public static void main(String[] args) {
        runQueries(List.of(1, 2, 3));
    }
}
