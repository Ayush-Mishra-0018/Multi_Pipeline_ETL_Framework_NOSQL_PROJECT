package com.example.pig.dataSetup;

import com.example.config.ConfigReader;
import com.example.model.MalformedRecord;
import com.example.model.PipelineExecutionResult;
import com.example.pig.service.PigInsertService;
import com.example.pig.util.BatchReader;
import com.example.pig.util.PigScriptExecutor;
import com.example.pig.util.PigServerManager;
import com.example.postgres.service.PostgresInsertService;
import com.example.postgres.service.PostgresSchemaInitializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public final class PigDataSetupExecutor {

    private static final String PIG_SCRIPT =
            "Pig-pipeline/src/main/resources/pig/parse_and_clean.pig";

    private PigDataSetupExecutor() {}

    public static PipelineExecutionResult execute(int input_batch_size) {

        long startTime = System.currentTimeMillis();

        PostgresSchemaInitializer.initialize("pig");

        boolean shouldClear = Boolean.parseBoolean(
                ConfigReader.get("mongo.clear.before.run", "true"));

        if (shouldClear) {
            PigInsertService.clearData();
        }

        String filePathsStr = ConfigReader.get("input.file.paths");
        String[] filePaths = filePathsStr.split(",");

        int batchSize = input_batch_size;
        if (input_batch_size <= 0) {
            batchSize = Integer.parseInt(
                    ConfigReader.get("batch.size", "10000"));
        }

        int batchId = 1;
        AtomicLong totalRecordsProcessed = new AtomicLong(0);
        AtomicLong totalMalformed = new AtomicLong(0);
        AtomicLong totalValid = new AtomicLong(0);
        AtomicInteger totalBatches = new AtomicInteger(0);

        List<MalformedRecord> malformedRecords = Collections.synchronizedList(new ArrayList<>());

        int numThreads = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<?>> futures = new ArrayList<>();

        try {
            for (String filePath : filePaths) {
                filePath = filePath.trim();
                System.out.println("Processing file: " + filePath);

                try (BatchReader reader = new BatchReader(filePath)) {

                    while (true) {
                        List<String> rawLines = reader.readNextBatch(batchSize);

                        if (rawLines.isEmpty()) break;

                        // Write raw batch to disk (unchanged)
                        final int currentBatchId = batchId++;
                        final int rawLinesSize = rawLines.size();
                        String rawBatchFile = PigInsertService.writeRawBatch(rawLines, currentBatchId);

                        String validOutput    = "./pig_data/valid/batch_"     + currentBatchId;
                        String malformedOutput = "./pig_data/malformed/batch_" + currentBatchId;

                        futures.add(executor.submit(() -> {
                            try {
                                // runs the pig script through the embedded PigServer (Hadoop MapReduce local mode)
                                PigScriptExecutor.executeSetupScript(
                                        PIG_SCRIPT, rawBatchFile,
                                        validOutput, malformedOutput, currentBatchId);

                                // Read Pig outputs for metadata (unchanged)
                                long validCount = countLinesInDirectory(validOutput);
                                List<String> malformedLines = readLinesFromDirectory(malformedOutput);

                                for (String mLine : malformedLines) {
                                    malformedRecords.add(MalformedRecord.builder()
                                            .batchId(currentBatchId)
                                            .line(mLine)
                                            .build());
                                }

                                totalRecordsProcessed.addAndGet(rawLinesSize);
                                totalMalformed.addAndGet(malformedLines.size());
                                totalValid.addAndGet(validCount);
                                totalBatches.incrementAndGet();

                                System.out.println("Batch " + currentBatchId + " processed by Pig. "
                                        + "(Valid: " + validCount
                                        + ", Malformed: " + malformedLines.size() + ")");
                            } catch (Exception e) {
                                System.err.println("Error processing batch " + currentBatchId);
                                e.printStackTrace();
                                throw new RuntimeException(e);
                            }
                        }));
                    }
                }
            }

            // Wait for all batch tasks to complete
            for (Future<?> future : futures) {
                future.get();
            }

            // Sort malformed records by batchId to ensure determinism
            malformedRecords.sort(java.util.Comparator.comparingInt(MalformedRecord::getBatchId));

            System.out.println("\nTotal malformed records: " + malformedRecords.size());

            long endTime  = System.currentTimeMillis();
            long totalTime = endTime - startTime;
            double avgBatchSize = totalBatches.get() == 0
                    ? 0 : (double) totalRecordsProcessed.get() / totalBatches.get();

            java.util.Map<String, Object> metadata = new java.util.HashMap<>();
            metadata.put("totalRecords",    (int) totalRecordsProcessed.get());
            metadata.put("totalValid",      (int) totalValid.get());
            metadata.put("totalMalformed",  (int) totalMalformed.get());
            metadata.put("totalBatches",    totalBatches.get());
            metadata.put("avgBatchSize",    avgBatchSize);
            metadata.put("executionTimeMs", (int) totalTime);

            System.out.println("\nPig pipeline execution completed.");

            return PipelineExecutionResult.builder()
                    .executionTime(totalTime)
                    .malformedRecords(malformedRecords)
                    .metadata(metadata)
                    .build();

        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            // Clean up threads and PigServers in the pool
            if (executor != null) {
                // Submit tasks to shut down the thread-local PigServer on each executor thread
                List<Future<?>> cleanupFutures = new ArrayList<>();
                for (int i = 0; i < numThreads; i++) {
                    cleanupFutures.add(executor.submit(() -> {
                        PigServerManager.close();
                        return null;
                    }));
                }
                for (Future<?> f : cleanupFutures) {
                    try {
                        f.get(1, TimeUnit.SECONDS); // brief timeout to not block
                    } catch (Exception e) {
                        // ignore
                    }
                }
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException ie) {
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
            // Cleanly shut down the PigServer that was used by this (main) thread.
            PigServerManager.close();
        }

        return PipelineExecutionResult.builder()
                .executionTime(0)
                .malformedRecords(new ArrayList<>())
                .build();
    }

    // -------------------------------------------------------------------------
    // File-system helpers — identical to original
    // -------------------------------------------------------------------------

    private static long countLinesInDirectory(String dirPath) throws IOException {
        long count = 0;
        File dir = new File(dirPath);
        if (!dir.exists() || !dir.isDirectory()) return 0;

        File[] files = dir.listFiles();
        if (files == null) return 0;

        for (File file : files) {
            if (file.isFile() && !file.getName().startsWith(".")) {
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    while (reader.readLine() != null) count++;
                }
            }
        }
        return count;
    }

    private static List<String> readLinesFromDirectory(String dirPath) throws IOException {
        List<String> lines = new ArrayList<>();
        File dir = new File(dirPath);
        if (!dir.exists() || !dir.isDirectory()) return lines;

        File[] files = dir.listFiles();
        if (files == null) return lines;

        for (File file : files) {
            if (file.isFile() && !file.getName().startsWith(".")) {
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = reader.readLine()) != null) lines.add(line);
                }
            }
        }
        return lines;
    }

    public static void main(String[] args) {
        execute(-1);
    }
}