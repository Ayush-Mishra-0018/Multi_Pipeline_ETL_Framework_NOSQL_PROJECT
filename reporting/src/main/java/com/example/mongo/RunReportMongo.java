package com.example.mongo;

import com.example.config.ConfigReader;
import com.example.model.MalformedRecord;
import com.example.model.PipelineExecutionResult;
import com.example.mongo.dataSetup.MongoDataSetupExecutor;
import com.example.mongo.runner.QueryRunner;
import com.example.reporting.postgres.PostgresReportService;
import org.bson.Document;
import java.util.List;
import java.sql.*;
import com.example.postgres.service.PostgresInsertService;


public class RunReportMongo {

    public static void reporting(
            List<Integer> queries
    ) {

        try {

            // =========================================
            // RUN PIPELINE
            // =========================================

            PipelineExecutionResult executionResult =
                    MongoDataSetupExecutor.execute();

            long pipelineRuntime =
                    executionResult.getExecutionTime();

            List<MalformedRecord> malformedRecords =
                    executionResult.getMalformedRecords();

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
                    com.example.mongo.MongoReportService
                            .getLatestPipelineMetadata();
            // #########################
            // #########################
            // #########################
            // write this latest data in global in postgres
            // #########################
            // #########################
            // #########################

            PostgresInsertService.insertGlobalMetadata(
                    "mongodb",
                    queries,
                    totalRuntime,
                    meta
            );
            PostgresInsertService.insertMalformed("mongodb",malformedRecords);



//            PostgresReportService.printStoredResults(
//
//                    "mongodb",
//                    queries
//            );


        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}