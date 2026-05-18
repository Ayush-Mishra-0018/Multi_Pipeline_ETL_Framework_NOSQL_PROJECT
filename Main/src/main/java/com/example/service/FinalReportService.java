package com.example.service;

import com.example.postgres.service.PostgresReaderService;

import java.util.Arrays;

public class FinalReportService {

    public static void generateFinalReport() {
        System.out.println("\n======================================================================================================================================================");
        System.out.println("                                                            FINAL EXECUTION REPORT");
        System.out.println("======================================================================================================================================================");
        
        System.out.println("\n>>> PIPELINE EXECUTION SUMMARY <<<");
        PostgresReaderService.readAllRowsPretty("global_db", Arrays.asList("run_metadata"));

        System.out.println("\n>>> BATCH PROCESSING DETAILS <<<");
        PostgresReaderService.readAllRowsPretty("global_db", Arrays.asList("batch_metadata"));
    }
}
