package com.example.reporting;

import com.example.config.ConfigReader;
import com.example.mongo.runner.QueryRunner;
import com.example.mongo.service.MongoConnection;
import com.example.postgres.service.PostgresReaderService;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Arrays;

import static com.mongodb.client.model.Sorts.descending;

public class RunModule {

    public static void reporting(long startTime) {

//        long startTime = System.currentTimeMillis();

        try {

            // =========================================
            // RUN ALL QUERIES
            // =========================================
            long queryStartTime = System.currentTimeMillis();

            QueryRunner.runQueries();

            long queryEndTime = System.currentTimeMillis();

            long queryRuntime =
                    queryEndTime - queryStartTime;

            long totalRuntime =
                    queryEndTime - startTime;

            int batchSize = Integer.parseInt(
                    ConfigReader.get("batch.size")
            );


            // =========================================
            // READ LATEST METADATA FROM MONGODB
            // =========================================
            MongoDatabase database =
                    MongoConnection.getDatabase();

            MongoCollection<Document> metaCollection =
                    database.getCollection(
                            "pipeline_run_metadata"
                    );

            Document meta =
                    metaCollection
                            .find()
                            .sort(descending("_id"))
                            .first();


            // =========================================
            // NICE HEADER
            // =========================================
            System.out.println(
                    "\n=============================================================="
            );
            System.out.println(
                    "               FINAL REPORTING DASHBOARD"
            );
            System.out.println(
                    "=============================================================="
            );
            System.out.println(
                    "\n=============================================================="
            );
            System.out.println(
                    "POSTGRES STORED RESULTS"
            );
            System.out.println(
                    "=============================================================="
            );

            PostgresReaderService.readAllRowsPretty(
                    Arrays.asList(
                            "query_1",
                            "query_2",
                            "query_3"
                    )
            );


            // =========================================
            // EXECUTION METADATA
            // =========================================
            System.out.println("\nEXECUTION METADATA");
            System.out.println(
                    "--------------------------------------------------------------"
            );

            if (meta != null) {

                System.out.printf(
                        "%-25s : %s%n",
                        "Pipeline name",
                        "MongoDB"
                );

                System.out.printf(
                        "%-25s : %s%n",
                        "Batch Size",
                        batchSize
                );

                System.out.printf(
                        "%-25s : %s%n",
                        "Average Batch Size",
                        meta.get("avgBatchSize")
                );

            }


            System.out.printf(
                    "%-25s : %d ms%n",
                    "Total Pipeline Runtime",
                    totalRuntime
            );


            // =========================================
            // READ TOP 5 ROWS FROM POSTGRES TABLES
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