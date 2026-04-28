package com.example.reporting;

import com.example.config.ConfigReader;
import com.example.model.BatchResult;
import com.example.mongo.service.MongoConnection;
import com.example.mongo.service.MongoInsertService;
import com.example.util.BatchProcessor;
import com.example.util.BatchReader;

import java.util.List;

public class MongoPipelineMain {

    public static void main(String[] args) {

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

                        MongoInsertService.insertBatchMetadata(result);
                        MongoInsertService.insertParsedLogs(result);
                        MongoInsertService.insertFilteredLogs(result);

                        totalRecordsProcessed += result.getTotalRecords();
                        totalMalformed += result.getMalformedRecords();
                        totalValid += result.getValidRecords();
                        totalBatches++;

                        batchId++;
                    }
                }
            }

            long endTime = System.currentTimeMillis();
            long totalTime = endTime - startTime;

            double avgBatchSize =
                    totalBatches == 0 ? 0 :
                            (double) totalRecordsProcessed / totalBatches;

            MongoInsertService.insertPipelineSummary(
                    totalRecordsProcessed,
                    totalValid,
                    totalMalformed,
                    totalBatches,
                    avgBatchSize,
                    totalTime
            );

            RunModule.reporting(startTime);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            MongoConnection.close();
        }
    }
}