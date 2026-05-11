package com.example.reporting.mongo;

import com.example.config.ConfigReader;
import com.example.mongo.dataSetup.MongoDataSetupExecutor;
import com.example.mongo.runner.QueryRunner;
import com.example.reporting.postgres.PostgresReportService;
import org.bson.Document;

import java.util.List;

public class RunReportMongo {

    public static void reporting(
            List<Integer> queries,
            int newRunId // use this for global insert functions
    ) {

        try {

            // =========================================
            // RUN PIPELINE
            // =========================================

            long pipelineRuntime =
                    MongoDataSetupExecutor.execute();

            // =========================================
            // RUN QUERIES
            // =========================================

            long queryRuntime =
                    QueryRunner.runQueries(
                            queries
                    );

            long totalRuntime =
                    pipelineRuntime + queryRuntime;

            int batchSize =
                    Integer.parseInt(
                            ConfigReader.get(
                                    "batch.size"
                            )
                    );

            // =========================================
            // FETCH METADATA
            // =========================================

            Document meta =
                    MongoReportService
                            .getLatestPipelineMetadata();
            // #########################
            // #########################
            // #########################
            // write this latest data in global in postgres
            // #########################
            // #########################
            // #########################

            // =========================================
            // HEADER
            // =========================================

            System.out.println(
                    "\n=============================================================="
            );

            System.out.println(
                    "         FINAL REPORTING DASHBOARD - MONGODB"
            );

            System.out.println(
                    "=============================================================="
            );

            // =========================================
            // POSTGRES RESULTS
            // =========================================

            System.out.println(
                    "\n=============================================================="
            );

            System.out.println(
                    "POSTGRES STORED RESULTS"
            );

            System.out.println(
                    "=============================================================="
            );

            PostgresReportService.printStoredResults(

                    "mongodb",
                    queries
            );
            // to print global call the function again.

            // =========================================
            // EXECUTION METADATA
            // =========================================

            System.out.println(
                    "\nEXECUTION METADATA"
            );

            System.out.println(
                    "--------------------------------------------------------------"
            );

            System.out.printf(
                    "%-25s : %s%n",
                    "Pipeline",
                    "MongoDB"
            );

            System.out.printf(
                    "%-25s : %s%n",
                    "Executed Queries",
                    queries
            );

            System.out.printf(
                    "%-25s : %d%n",
                    "Batch Size",
                    batchSize
            );

            if (meta != null) {

                System.out.printf(
                        "%-25s : %s%n",
                        "Average Batch Size",
                        meta.get("avgBatchSize")
                );
            }

            System.out.printf(
                    "%-25s : %d ms%n",
                    "Pipeline Runtime",
                    pipelineRuntime
            );

            System.out.printf(
                    "%-25s : %d ms%n",
                    "Query Runtime",
                    queryRuntime
            );

            System.out.printf(
                    "%-25s : %d ms%n",
                    "Total Runtime",
                    totalRuntime
            );

            // =========================================
            // DONE
            // =========================================

            System.out.println(
                    "\n=============================================================="
            );

            System.out.println(
                    "REPORT COMPLETED SUCCESSFULLY"
            );

            System.out.println(
                    "=============================================================="
            );

        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}