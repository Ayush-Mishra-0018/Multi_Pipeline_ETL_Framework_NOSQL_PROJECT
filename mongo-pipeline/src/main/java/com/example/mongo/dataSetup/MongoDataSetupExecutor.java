package com.example.mongo.dataSetup;

import com.example.config.ConfigReader;
import com.example.model.BatchResult;
import com.example.model.MalformedRecord;
import com.example.model.PipelineExecutionResult;
import com.example.mongo.service.MongoInsertService;
import com.example.util.BatchProcessor;
import com.example.util.BatchReader;

import java.util.ArrayList;
import java.util.List;

public final class MongoDataSetupExecutor {

    private MongoDataSetupExecutor() {
    }

    public static PipelineExecutionResult execute() {

        long startTime =
                System.currentTimeMillis();

        boolean shouldClear =
                Boolean.parseBoolean(
                        ConfigReader.get(
                                "mongo.clear.before.run",
                                "true"
                        )
                );

        if (shouldClear) {

            MongoInsertService.clearCollections();
        }

        String filePathsStr =
                ConfigReader.get("input.file.paths");

        String[] filePaths =
                filePathsStr.split(",");

        int batchSize =
                Integer.parseInt(
                        ConfigReader.get(
                                "batch.size",
                                "10000"
                        )
                );
        System.out.println("\n\n");
        System.out.println(batchSize);
        System.out.println("\n\n");

        int batchId = 1;

        long totalRecordsProcessed = 0;
        long totalMalformed = 0;
        long totalValid = 0;
        int totalBatches = 0;

        // =====================================
        // GLOBAL MALFORMED RECORDS LIST
        // =====================================

        List<MalformedRecord> malformedRecords =
                new ArrayList<>();

        try {

            for (String filePath : filePaths) {

                filePath = filePath.trim();

                System.out.println(
                        "Processing file: " + filePath
                );

                try (
                        BatchReader reader =
                                new BatchReader(filePath)
                ) {

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

                        // =====================================
                        // COLLECT MALFORMED RECORDS
                        // =====================================

                        malformedRecords.addAll(
                                result.getMalformedLogs()
                        );

                        // =====================================
                        // INSERT INTO MONGO
                        // =====================================

                        MongoInsertService.insertBatchMetadata(
                                result
                        );

                        MongoInsertService.insertParsedLogs(
                                result
                        );

                        MongoInsertService.insertFilteredLogs(
                                result
                        );

                        // =====================================
                        // UPDATE GLOBAL METRICS
                        // =====================================

                        totalRecordsProcessed +=
                                result.getTotalRecords();

                        totalMalformed +=
                                result.getMalformedRecords();

                        totalValid +=
                                result.getValidRecords();

                        totalBatches++;

                        System.out.println(
                                "Batch " + batchId +
                                        " inserted successfully."
                        );

                        batchId++;
                    }
                }
            }

            // =====================================
            // PRINT MALFORMED RECORD SUMMARY
            // =====================================

            System.out.println(
                    "\nTotal malformed records: " +
                            malformedRecords.size()
            );

            // =====================================
            // STORE FINAL SUMMARY
            // =====================================

            long endTime =
                    System.currentTimeMillis();

            long totalTime =
                    endTime - startTime;

            double avgBatchSize =
                    totalBatches == 0
                            ? 0
                            : (double) totalRecordsProcessed
                            / totalBatches;

            MongoInsertService.insertPipelineSummary(
                    totalRecordsProcessed,
                    totalValid,
                    totalMalformed,
                    totalBatches,
                    avgBatchSize,
                    totalTime
            );

            System.out.println(
                    "\nMongo pipeline execution completed."
            );

            return PipelineExecutionResult.builder()
                    .executionTime(totalTime)
                    .malformedRecords(malformedRecords)
                    .build();

        } catch (Exception e) {

            e.printStackTrace();
        }

        return PipelineExecutionResult.builder()
                .executionTime(0)
                .malformedRecords(new ArrayList<>())
                .build();
    }
}