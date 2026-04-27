package com.example.mongo.runner;

import com.example.config.ConfigReader;
import com.example.model.BatchResult;
import com.example.mongo.service.MongoConnection;
import com.example.mongo.service.MongoInsertService;
import com.example.util.BatchProcessor;
import com.example.util.BatchReader;

import java.util.List;

public class MongoPipelineMain {

    public static void main(String[] args) {


        boolean shouldClear = Boolean.parseBoolean(

                ConfigReader.get("mongo.clear.before.run", "false")

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
        int totalInserted = 0;

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

                        totalInserted += result.getValidRecords();

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

            System.out.println(
                    "Pipeline completed successfully. " +
                            "Total valid records inserted = " +
                            totalInserted
            );

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            MongoConnection.close();
        }
    }
}