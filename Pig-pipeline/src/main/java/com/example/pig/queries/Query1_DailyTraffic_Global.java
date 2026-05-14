package com.example.pig.queries;

import com.example.pig.util.PigScriptExecutor;
import com.example.postgres.service.PostgresInsertService;
import org.bson.Document;

import java.io.File;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

public class Query1_DailyTraffic_Global {

    private static final String SCRIPT_PATH = "Pig-pipeline/src/main/resources/pig/query1.pig";

    public static void run() {

        String pipelineName = "pig";
        String runId = UUID.randomUUID().toString();
        java.sql.Timestamp executedAt = java.sql.Timestamp.from(Instant.now());

        File validDir = new File("./pig_data/valid");
        List<String> batchCollections = new ArrayList<>();
        
        if (validDir.exists() && validDir.isDirectory()) {
            File[] batches = validDir.listFiles();
            if (batches != null) {
                for (File batch : batches) {
                    if (batch.isDirectory() && batch.getName().startsWith("batch_")) {
                        batchCollections.add(batch.getName());
                    }
                }
            }
        }

        ExecutorService executor =
                Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        List<Future<List<Document>>> futures = new ArrayList<>();

        Map<String, Document> finalMap = new HashMap<>();
        Map<String, Set<Integer>> batchTracker = new HashMap<>();

        // =========================
        // STEP 1: PER-BATCH QUERY
        // =========================
        for (String batchDirName : batchCollections) {

            futures.add(executor.submit(() -> {

                int batchId = Integer.parseInt(
                        batchDirName.substring(batchDirName.lastIndexOf("_") + 1)
                );

                String inputDir = "./pig_data/valid/" + batchDirName;
                String outputDir = "./pig_data/query1_out/" + batchDirName;
                
                // Ensure output dir doesn't exist
                PigScriptExecutor.deleteDirectory(new File(outputDir));

                // Execute Pig Script
                PigScriptExecutor.executeQueryScript(SCRIPT_PATH, inputDir, outputDir, batchId);

                // Read output
                List<String> lines = PigScriptExecutor.readOutputLines(outputDir);
                
                List<Document> docs = new ArrayList<>();
                for (String line : lines) {
                    try {
                        String[] parts = line.split("\t");
                        if (parts.length >= 5 && !parts[1].isEmpty()) {
                            Document doc = new Document();
                            doc.append("log_date", parts[0]);
                            doc.append("status_code", Integer.parseInt(parts[1]));
                            doc.append("request_count", Integer.parseInt(parts[2]));
                            doc.append("total_bytes", Long.parseLong(parts[3]));
                            doc.append("batch_id", Integer.parseInt(parts[4]));
                            docs.add(doc);
                        }
                    } catch (Exception e) {
                        System.err.println("Skipping malformed line: " + line);
                    }
                }

                // Clean up output dir to save space
                PigScriptExecutor.deleteDirectory(new File(outputDir));

                return docs;
            }));
        }

        executor.shutdown();
        try {
            executor.awaitTermination(15, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // =========================
        // STEP 2: MERGE RESULTS
        // =========================
        for (Future<List<Document>> future : futures) {
            try {
                List<Document> partialResults = future.get();

                for (Document doc : partialResults) {

                    String date = doc.getString("log_date");
                    int status = doc.getInteger("status_code");
                    int batchId = doc.getInteger("batch_id");

                    String key = date + "_" + status;

                    int count = doc.getInteger("request_count");
                    long bytes = doc.getLong("total_bytes");

                    if (!finalMap.containsKey(key)) {

                        finalMap.put(key,
                                new Document("log_date", date)
                                        .append("status_code", status)
                                        .append("request_count", count)
                                        .append("total_bytes", bytes)
                        );

                        batchTracker.put(key, new HashSet<>());

                    } else {

                        Document existing = finalMap.get(key);

                        existing.put("request_count",
                                existing.getInteger("request_count") + count);

                        existing.put("total_bytes",
                                existing.getLong("total_bytes") + bytes);
                    }

                    batchTracker.get(key).add(batchId);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // =========================
        // STEP 3: SORT
        // =========================
        List<Document> output = new ArrayList<>(finalMap.values());

        output.sort(
                Comparator.comparing((Document d) -> d.getString("log_date"))
                        .thenComparing(d -> d.getInteger("status_code"))
        );

        // =========================
        // STEP 4: BUILD ROWS
        // =========================
        List<Map<String, Object>> rows = new ArrayList<>();

        for (Document doc : output) {

            String key =
                    doc.getString("log_date") + "_" +
                            doc.getInteger("status_code");

            Set<Integer> batches = batchTracker.get(key);

            List<Integer> sortedBatches = new ArrayList<>(batches);
            Collections.sort(sortedBatches);

            String batchString =
                    String.join("+",
                            sortedBatches.stream()
                                    .map(String::valueOf)
                                    .toArray(String[]::new)
                    );

            // Output from Pig is already yyyy-MM-dd
            String formattedDate = doc.getString("log_date");
            java.sql.Date sqlDate = java.sql.Date.valueOf(formattedDate);

            Map<String, Object> row = new LinkedHashMap<>();

            row.put("log_date", sqlDate);
            row.put("status_code", doc.getInteger("status_code"));
            row.put("request_count", doc.getInteger("request_count"));
            row.put("total_bytes", doc.getLong("total_bytes"));
            row.put("batch_id", batchString);

            rows.add(row);
        }

        // DEBUG
        System.out.println("Rows to insert: " + rows.size());

        // =========================
        // STEP 5: INSERT INTO POSTGRES
        // =========================
        PostgresInsertService.Insert( 
                "pig",
                "query_1",
                rows
        );

        // =========================
        // STEP 6: PRINT OUTPUT
        // =========================
        System.out.printf(
                "%-12s | %-12s | %-15s | %-15s | %-10s%n",
                "log_date", "status_code", "request_count", "total_bytes",
                "batches"
        );

        System.out.println("--------------------------------------------------------------------------------------------------------------");

        for (Map<String, Object> row : rows) {

            System.out.printf(
                    "%-12s | %-12d | %-15d | %-15d | %-10s%n",
                    row.get("log_date"),
                    row.get("status_code"),
                    row.get("request_count"),
                    row.get("total_bytes"),
                    row.get("batch_id")
            );
        }
    }
}
