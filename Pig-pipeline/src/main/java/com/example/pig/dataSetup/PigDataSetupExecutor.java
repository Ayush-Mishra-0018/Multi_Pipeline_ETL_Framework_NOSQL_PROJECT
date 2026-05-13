package com.example.pig.dataSetup;

import com.example.config.ConfigReader;
import com.example.model.BatchResult;
import com.example.model.MalformedRecord;
import com.example.model.PipelineExecutionResult;
import com.example.pig.service.PigInsertService;
import com.example.pig.util.BatchProcessor;
import com.example.pig.util.BatchReader;
import com.example.postgres.service.PostgresInsertService;

import java.util.ArrayList;
import java.util.List;

public final class PigDataSetupExecutor {

    private PigDataSetupExecutor() {
    }

    public static PipelineExecutionResult execute() {

        long startTime = System.currentTimeMillis();

        boolean shouldClear = Boolean.parseBoolean(
                ConfigReader.get("mongo.clear.before.run", "true")
        );

        if (shouldClear) {
            PigInsertService.clearData();
        }

        String filePathsStr = ConfigReader.get("input.file.paths");
        String[] filePaths = filePathsStr.split(",");

        int batchSize = Integer.parseInt(
                ConfigReader.get("batch.size", "10000")
        );

        int batchId = 1;
        long totalRecordsProcessed = 0;
        long totalMalformed = 0;
        long totalValid = 0;
        int totalBatches = 0;

        List<MalformedRecord> malformedRecords = new ArrayList<>();

        try {
            for (String filePath : filePaths) {
                filePath = filePath.trim();
                System.out.println("Processing file: " + filePath);

                try (BatchReader reader = new BatchReader(filePath)) {
                    while (true) {
                        List<String> rawLines = reader.readNextBatch(batchSize);

                        if (rawLines.isEmpty()) {
                            break;
                        }

                        BatchResult result = BatchProcessor.processBatch(rawLines, batchId);

                        malformedRecords.addAll(result.getMalformedLogs());

                        PigInsertService.insertParsedLogs(result);

                        totalRecordsProcessed += result.getTotalRecords();
                        totalMalformed += result.getMalformedRecords();
                        totalValid += result.getValidRecords();
                        totalBatches++;

                        System.out.println("Batch " + batchId + " written to TSV successfully.");
                        batchId++;
                    }
                }
            }

            System.out.println("\nTotal malformed records: " + malformedRecords.size());

            long endTime = System.currentTimeMillis();
            long totalTime = endTime - startTime;
            double avgBatchSize = totalBatches == 0 ? 0 : (double) totalRecordsProcessed / totalBatches;

            // Optional: You could save pipeline summary metadata here directly if PostgresInsertService requires Document.
            // Since PostgresInsertService uses org.bson.Document, we'll create a Document to match mongo pipeline's behavior.
            org.bson.Document metadata = new org.bson.Document()
                    .append("totalRecords", (int) totalRecordsProcessed)
                    .append("totalValid", (int) totalValid)
                    .append("totalMalformed", (int) totalMalformed)
                    .append("totalBatches", totalBatches)
                    .append("avgBatchSize", avgBatchSize)
                    .append("executionTimeMs", (int) totalTime);

            PostgresInsertService.insertGlobalMetadata(
                    "Pig",
                    List.of(1, 2, 3), // Indicates all queries or standard data setup run
                    totalTime,
                    metadata
            );

            PostgresInsertService.insertMalformed("global_db", malformedRecords);

            System.out.println("\nPig pipeline execution completed.");

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

    public static void main(String[] args) {
        execute();
    }
}
