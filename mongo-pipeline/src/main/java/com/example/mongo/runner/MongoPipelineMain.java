package com.example.mongo.runner;

import com.example.config.ConfigReader;
import com.example.model.BatchResult;
import com.example.mongo.queries.Query3_HourlyErrorAnalysis;
import com.example.mongo.service.MongoConnection;
import com.example.mongo.service.MongoInsertService;
import com.example.util.BatchProcessor;
import com.example.util.BatchReader;

import java.util.List;

public class MongoPipelineMain {

    public static void main(String[] args) {

        // 🚀 START TIME
        long startTime = System.currentTimeMillis();

        boolean shouldClear = Boolean.parseBoolean(
                ConfigReader.get("mongo.clear.before.run", "true")
        );

        if (shouldClear) {
            MongoInsertService.clearCollections();
        }

        String filePathsStr = ConfigReader.get("input.file.paths");
        String[] filePaths = filePathsStr.split(",");

        int batchSize = Integer.parseInt(
                ConfigReader.get("batch.size", "10000")
        );

        int batchId = 1;

        // 🚀 GLOBAL METRICS
        long totalRecordsProcessed = 0;
        long totalMalformed = 0;
        long totalValid = 0;
        int totalBatches = 0;

        try {

            for (String filePath : filePaths) {

                filePath = filePath.trim();

                System.out.println("Processing file: " + filePath);

                try (BatchReader reader = new BatchReader(filePath)) {

                    while (true) {

                        List<String> rawLines =
                                reader.readNextBatch(batchSize);

                        if (rawLines.isEmpty()) {
                            break;
                        }

                        BatchResult result =
                                BatchProcessor.processBatch(
                                        rawLines,
                                        batchId
                                );

                        // 🚀 INSERT INTO MONGO
                        MongoInsertService.insertBatchMetadata(result);
                        MongoInsertService.insertParsedLogs(result);
                        MongoInsertService.insertFilteredLogs(result);

                        // 🚀 UPDATE GLOBAL METRICS
                        totalRecordsProcessed += result.getTotalRecords();
                        totalMalformed += result.getMalformedRecords();
                        totalValid += result.getValidRecords();
                        totalBatches++;

                        System.out.println(
                                "Batch " + batchId +
                                        " inserted | Total Records = " +
                                        result.getTotalRecords() +
                                        " | Malformed = " +
                                        result.getMalformedRecords()
                        );

                        batchId++;
                    }
                }
            }

            // 🚀 END TIME
            long endTime = System.currentTimeMillis();
            long totalTime = endTime - startTime;

            double avgBatchSize = totalBatches == 0
                    ? 0
                    : (double) totalRecordsProcessed / totalBatches;

            // 🚀 PRINT FINAL SUMMARY
            System.out.println("\n======== PIPELINE SUMMARY ========");
            System.out.println("Total Records Processed: " + totalRecordsProcessed);
            System.out.println("Total Valid Records: " + totalValid);
            System.out.println("Total Malformed Records: " + totalMalformed);
            System.out.println("Total Batches: " + totalBatches);
            System.out.println("Average Batch Size: " + avgBatchSize);
            System.out.println("Total Execution Time (ms): " + totalTime);

            // 🚀 STORE SUMMARY IN MONGO
            MongoInsertService.insertPipelineSummary(
                    totalRecordsProcessed,
                    totalValid,
                    totalMalformed,
                    totalBatches,
                    avgBatchSize,
                    totalTime
            );

            System.out.println(
                    "\nPipeline completed successfully. " +
                            "Total valid records inserted = " +
                            totalValid
            );

            QueryRunner.runQuery3();
//            QueryRunner.runQueries();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            MongoConnection.close();
        }
    }
}