package com.example.hive.dataSetup;

import com.example.config.ConfigReader;
import com.example.hive.service.HdfsUploader;
import com.example.hive.service.HiveProcessRunner;
import com.example.hive.service.HiveScriptBuilder;
import com.example.model.MalformedRecord;
import com.example.model.PipelineExecutionResult;

import java.util.ArrayList;
import java.util.List;

public final class HiveDataSetupExecutor {

    // How many malformed records to pull from Hive for Postgres storage
    private static final int MALFORMED_SAMPLE_LIMIT = 500;

    private HiveDataSetupExecutor() {
    }

    public static PipelineExecutionResult execute() {

        long startTime = System.currentTimeMillis();

        List<MalformedRecord> malformedRecords = new ArrayList<>();

        try {

            // ======================================================
            // STEP 1: Resolve input file paths from config
            // ======================================================

            String filePathsStr = ConfigReader.get("input.file.paths");

            if (filePathsStr == null || filePathsStr.isBlank()) {
                throw new RuntimeException(
                        "[HiveDataSetup] input.file.paths not set in app.properties"
                );
            }

            String[] filePaths = filePathsStr.split(",");

            // ======================================================
            // STEP 2: Dynamic Batching and Upload to HDFS
            // ======================================================

            System.out.println("\n[HiveDataSetup] Pre-processing batches and uploading to HDFS...");

            String batchSizeStr = ConfigReader.get("batch.size");
            int batchSize = (batchSizeStr != null && !batchSizeStr.isBlank()) ? Integer.parseInt(batchSizeStr) : 10000;

            java.io.File stagedFile = new java.io.File("staged_batches.txt");
            int currentBatchId = 1;
            long lineCount = 0;

            try (java.io.BufferedWriter writer = new java.io.BufferedWriter(new java.io.FileWriter(stagedFile))) {
                for (String pathStr : filePaths) {
                    try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(pathStr.trim()))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            writer.write(currentBatchId + "\000" + line);
                            writer.newLine();
                            lineCount++;
                            if (lineCount >= batchSize) {
                                currentBatchId++;
                                lineCount = 0;
                            }
                        }
                    }
                    

                    if (lineCount > 0) {
                        currentBatchId++;
                        lineCount = 0;
                    }
                }
            }

            String stagedHdfsDir = "/nasa_raw/staged";
            HdfsUploader.upload("staged_batches.txt", stagedHdfsDir);

            List<String> hiveConfs = new ArrayList<>();
            hiveConfs.add(HiveScriptBuilder.hdfsBaseConf());

            int finalTotalBatches = (lineCount == 0 && currentBatchId > 1) ? currentBatchId - 1 : currentBatchId;

            System.out.println("[HiveDataSetup] Upload complete.");



            System.out.println("\n[HiveDataSetup] Step 3: Executing hive_setup.hql...");
            HiveProcessRunner.runScript(
                    HiveScriptBuilder.getScriptPath("hive_setup.hql"),
                    hiveConfs.toArray(new String[0])
            );

            System.out.println(
                    "[HiveDataSetup] All ETL scripts complete. " +
                    "nasa_filtered_logs is ready."
            );


            long totalRecords   = queryCount("nasa_raw_logs");
            long totalMalformed = queryCount("nasa_malformed_logs");
            long totalValid     = queryCount("nasa_filtered_logs");
            int  totalBatches   = finalTotalBatches;

            // ======================================================
            // STEP 5: Pull a SAMPLE of malformed lines from Hive
            //
            // Hive decides which records are malformed.
            // Java only carries the strings over to Postgres.
            // ======================================================

            System.out.println(
                    "\n[HiveDataSetup] Sampling malformed records from Hive..."
            );

            List<String> malformedLines =
                    HiveProcessRunner.runInline(
                            "SELECT batch_id, record " +
                                    "FROM nasa_malformed_logs " +
                                    "LIMIT " + MALFORMED_SAMPLE_LIMIT + ";",
                            HiveScriptBuilder.hdfsBaseConf()
                    );

            for (String line : malformedLines) {
                if (line.isBlank()) continue;

                // TSV: batch_id \t record
                String[] parts = line.split("\t", 2);

                // GUARD 1: A true Hive SELECT output row will always have exactly 2 parts separated by a tab.
                if (parts.length != 2) continue;

                try {
                    // GUARD 2: The first part MUST be a valid integer (the batch_id).
                    int batchId = Integer.parseInt(parts[0].trim());
                    String record = parts[1].trim();

                    malformedRecords.add(
                            MalformedRecord.builder()
                                    .batchId(batchId)
                                    .line(record)
                                    .build()
                    );
                } catch (NumberFormatException e) {
                    // If the first part isn't a number, it's a Hive log that happened to contain a tab. Skip it.
                    continue;
                }
            }

            long endTime   = System.currentTimeMillis();
            long totalTime = endTime - startTime;

            double avgBatchSize =
                    totalBatches == 0
                            ? 0
                            : (double) totalRecords / totalBatches;

            System.out.println(
                    "\n[HiveDataSetup] ─── Pipeline Summary ──────────────────"
            );
            System.out.printf("  Total Records   : %d%n",   totalRecords);
            System.out.printf("  Total Valid     : %d%n",   totalValid);
            System.out.printf("  Total Malformed : %d%n",   totalMalformed);
            System.out.printf("  Total Batches   : %d%n",   totalBatches);
            System.out.printf("  Avg Batch Size  : %.2f%n", avgBatchSize);
            System.out.printf("  Execution Time  : %d ms%n", totalTime);
            System.out.println(
                    "  ──────────────────────────────────────────────────"
            );

            return PipelineExecutionResult.builder()
                    .executionTime(totalTime)
                    .malformedRecords(malformedRecords)
                    .totalBatches(finalTotalBatches)
                    .build();

        } catch (Exception e) {

            e.printStackTrace();
            System.err.println(
                    "[HiveDataSetup] Pipeline failed: " + e.getMessage()
            );
        }

        return PipelineExecutionResult.builder()
                .executionTime(0)
                .malformedRecords(malformedRecords)
                .totalBatches(0)
                .build();
    }

    // ──────────────────────────────────────────────────────────────────
    // Helper: run COUNT(*) against a Hive table; return the count.
    // Hive computes; Java parses the single integer from the output.
    // ──────────────────────────────────────────────────────────────────

    private static long queryCount(String tableName) {

        try {

            List<String> lines =
                    HiveProcessRunner.runInline(
                            "SELECT COUNT(*) FROM " + tableName + ";",
                            HiveScriptBuilder.hdfsBaseConf()
                    );

            for (String line : lines) {

                if (line.isBlank()) continue;

                String trimmed = line.trim();
                if (!trimmed.matches("\\d+")) continue;

                try {
                    return Long.parseLong(trimmed);
                } catch (NumberFormatException ignored) {
                    // should never reach here after the regex guard
                }
            }

        } catch (Exception e) {
            System.err.println(
                    "[HiveDataSetup] Could not count " + tableName +
                            ": " + e.getMessage()
            );
        }

        return 0L;
    }
}
