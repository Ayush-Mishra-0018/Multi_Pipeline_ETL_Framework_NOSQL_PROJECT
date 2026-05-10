package com.example.menu;

public final class ConsoleMenu {

    private ConsoleMenu() {
    }

    public static void printWelcome() {

        System.out.println(
                "\n=================================================="
        );

        System.out.println(
                "         NOSQL ETL PIPELINE FRAMEWORK"
        );

        System.out.println(
                "=================================================="
        );
    }

    public static void printPipelines() {

        System.out.println(
                "\nAVAILABLE PIPELINES:"
        );

        System.out.println("1. MongoDB");
        System.out.println("2. Hive");
        System.out.println("3. Pig");
        System.out.println("4. MapReduce");
    }

    public static void printQueries() {

        System.out.println(
                "\nAVAILABLE QUERIES:"
        );

        System.out.println(
                "1. Daily Traffic Analysis"
        );

        System.out.println(
                "2. Top Requested Resources"
        );

        System.out.println(
                "3. Hourly Error Analysis"
        );
    }
}