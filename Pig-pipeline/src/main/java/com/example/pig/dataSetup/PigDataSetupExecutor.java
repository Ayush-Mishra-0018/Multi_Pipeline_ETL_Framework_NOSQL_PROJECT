package com.example.pig.dataSetup;

import com.example.config.ConfigReader;
import com.example.model.MalformedRecord;
import com.example.model.PipelineExecutionResult;
import com.example.pig.service.PigInsertService;
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
import java.util.List;

/**
 * Orchestrates the Pig parse-and-clean pipeline.
 *
 * <h3>What changed vs the original</h3>
 * <ul>
 *   <li>The private {@code executePigScript} method that spawned a {@code pig}
 *       CLI process via {@code ProcessBuilder} has been replaced by a call to
 *       {@link PigScriptExecutor#executeSetupScript}, which uses the embedded
 *       {@link org.apache.pig.PigServer} API.  This eliminates the per-batch
 *       JVM startup overhead.
 *   <li>A {@code finally} block calls {@link PigServerManager#close()} so the
 *       main-thread {@code PigServer} is cleanly shut down after all batches
 *       are processed.
 * </ul>
 *
 * <h3>What has NOT changed</h3>
 * All batch-reading, output-counting, malformed-record collection, Postgres
 * insertion, and result-building logic is identical to the original.
 */
public final class PigDataSetupExecutor {

    private static final String PIG_SCRIPT =
            "Pig-pipeline/src/main/resources/pig/parse_and_clean.pig";

    private PigDataSetupExecutor() {}

    public static PipelineExecutionResult execute() {

        long startTime = System.currentTimeMillis();

        PostgresSchemaInitializer.initialize("pig");

        boolean shouldClear = Boolean.parseBoolean(
                ConfigReader.get("mongo.clear.before.run", "true"));

        if (shouldClear) {
            PigInsertService.clearData();
        }

        String filePathsStr = ConfigReader.get("input.file.paths");
        String[] filePaths = filePathsStr.split(",");

        int batchSize = Integer.parseInt(
                ConfigReader.get("batch.size", "10000"));

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

                try (BufferedReader reader =
                             Files.newBufferedReader(Path.of(filePath), StandardCharsets.ISO_8859_1)) {

                    while (true) {
                        List<String> rawLines = new ArrayList<>(batchSize);
                        String line;
                        while (rawLines.size() < batchSize && (line = reader.readLine()) != null) {
                            rawLines.add(line);
                        }

                        if (rawLines.isEmpty()) break;

                        // Write raw batch to disk (unchanged)
                        String rawBatchFile = PigInsertService.writeRawBatch(rawLines, batchId);

                        String validOutput    = "./pig_data/valid/batch_"     + batchId;
                        String malformedOutput = "./pig_data/malformed/batch_" + batchId;

                        // ── KEY CHANGE ────────────────────────────────────────────────────
                        // Original: spawned  "pig -x local -param ... -f parse_and_clean.pig"
                        //           via ProcessBuilder — one new JVM per batch.
                        // New:      calls PigScriptExecutor.executeSetupScript(), which runs
                        //           the identical .pig script through the embedded PigServer
                        //           (Hadoop MapReduce local mode) inside this JVM.
                        // ─────────────────────────────────────────────────────────────────
                        PigScriptExecutor.executeSetupScript(
                                PIG_SCRIPT, rawBatchFile,
                                validOutput, malformedOutput, batchId);

                        // Read Pig outputs for metadata (unchanged)
                        long validCount = countLinesInDirectory(validOutput);
                        List<String> malformedLines = readLinesFromDirectory(malformedOutput);

                        for (String mLine : malformedLines) {
                            malformedRecords.add(MalformedRecord.builder()
                                    .batchId(batchId)
                                    .line(mLine)
                                    .build());
                        }

                        totalRecordsProcessed += rawLines.size();
                        totalMalformed        += malformedLines.size();
                        totalValid            += validCount;
                        totalBatches++;

                        System.out.println("Batch " + batchId + " processed by Pig. "
                                + "(Valid: " + validCount
                                + ", Malformed: " + malformedLines.size() + ")");
                        batchId++;
                    }
                }
            }

            System.out.println("\nTotal malformed records: " + malformedRecords.size());

            long endTime  = System.currentTimeMillis();
            long totalTime = endTime - startTime;
            double avgBatchSize = totalBatches == 0
                    ? 0 : (double) totalRecordsProcessed / totalBatches;

            java.util.Map<String, Object> metadata = new java.util.HashMap<>();
            metadata.put("totalRecords",    (int) totalRecordsProcessed);
            metadata.put("totalValid",      (int) totalValid);
            metadata.put("totalMalformed",  (int) totalMalformed);
            metadata.put("totalBatches",    totalBatches);
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
        execute();
    }
}