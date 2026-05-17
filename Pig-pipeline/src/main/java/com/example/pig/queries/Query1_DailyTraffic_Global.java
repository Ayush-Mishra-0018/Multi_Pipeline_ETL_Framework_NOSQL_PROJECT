package com.example.pig.queries;

import com.example.pig.util.PigScriptExecutor;
import com.example.pig.util.PigServerManager;
import com.example.postgres.service.PostgresInsertService;
import org.bson.Document;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

/**
 * Daily traffic aggregation per (date, status_code) across all valid batches.
 *
 * <h3>Execution model</h3>
 * Batches are processed <em>sequentially</em> on a single worker thread via
 * {@link Executors#newSingleThreadExecutor()}.  This is required because
 * {@link org.apache.pig.PigServer} in LOCAL mode is not safe to use
 * concurrently within the same JVM — Hadoop's shared {@code Configuration}
 * state causes non-deterministic failures when multiple Pig jobs run in
 * parallel.  Sequential execution guarantees deterministic results on every
 * run.
 *
 * <p>Performance is still significantly better than the original
 * {@code ProcessBuilder} approach because the single worker thread creates its
 * {@link org.apache.pig.PigServer} <em>once</em> and reuses it for every
 * batch — the 10–30 s per-batch JVM startup cost is paid exactly once.
 */
public class Query1_DailyTraffic_Global {

    private static final String SCRIPT_PATH =
            "Pig-pipeline/src/main/resources/pig/query1.pig";

    public static void run() {

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

        // Sort numerically so batches are always processed in order 1, 2, 3 …
        batchCollections.sort(Comparator.comparingInt(
                name -> Integer.parseInt(name.substring(name.lastIndexOf('_') + 1))));

        // Sequential execution: PigServer in LOCAL mode is not thread-safe.
        // A single worker thread creates its PigServer once and reuses it for
        // every batch — no per-batch JVM startup, no shared-state collisions.
        ExecutorService executor = Executors.newSingleThreadExecutor();

        List<Future<List<Document>>> futures = new ArrayList<>();

        Map<String, Document>    finalMap     = new HashMap<>();
        Map<String, Set<Integer>> batchTracker = new HashMap<>();

        // =========================
        // STEP 1: PER-BATCH QUERY
        // =========================
        for (String batchDirName : batchCollections) {

            futures.add(executor.submit(() -> {
                // No try/finally PigServerManager.close() here — the PigServer
                // is intentionally reused across all batches on this thread.
                int batchId = Integer.parseInt(
                        batchDirName.substring(batchDirName.lastIndexOf("_") + 1));

                String inputDir  = "./pig_data/valid/"     + batchDirName;
                String outputDir = "./pig_data/query1_out/" + batchDirName;

                PigScriptExecutor.deleteDirectory(new File(outputDir));
                PigScriptExecutor.executeQueryScript(SCRIPT_PATH, inputDir, outputDir, batchId);

                List<String> lines = PigScriptExecutor.readOutputLines(outputDir);

                List<Document> docs = new ArrayList<>();
                for (String line : lines) {
                    try {
                        String[] parts = line.split("\t");
                        if (parts.length >= 5 && !parts[1].isEmpty()) {
                            Document doc = new Document();
                            doc.append("log_date",      parts[0]);
                            doc.append("status_code",   Integer.parseInt(parts[1]));
                            doc.append("request_count", Integer.parseInt(parts[2]));
                            doc.append("total_bytes",   Long.parseLong(parts[3]));
                            doc.append("batch_id",      Integer.parseInt(parts[4]));
                            docs.add(doc);
                        }
                    } catch (Exception e) {
                        System.err.println("Skipping malformed line: " + line);
                    }
                }

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

        // Shut down the single worker thread's PigServer now that all batches
        // are done.  Called here (not inside each Callable) so the server is
        // reused across all batches rather than recreated for every one.
        PigServerManager.close();

        // =========================
        // STEP 2: MERGE RESULTS
        // =========================
        for (Future<List<Document>> future : futures) {
            try {
                List<Document> partialResults = future.get();

                for (Document doc : partialResults) {

                    String date    = doc.getString("log_date");
                    int    status  = doc.getInteger("status_code");
                    int    batchId = doc.getInteger("batch_id");
                    String key     = date + "_" + status;

                    int  count = doc.getInteger("request_count");
                    long bytes = doc.getLong("total_bytes");

                    if (!finalMap.containsKey(key)) {
                        finalMap.put(key,
                                new Document("log_date",      date)
                                        .append("status_code",   status)
                                        .append("request_count", count)
                                        .append("total_bytes",   bytes));
                        batchTracker.put(key, new HashSet<>());
                    } else {
                        Document existing = finalMap.get(key);
                        existing.put("request_count", existing.getInteger("request_count") + count);
                        existing.put("total_bytes",   existing.getLong("total_bytes")   + bytes);
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
                        .thenComparing(d -> d.getInteger("status_code")));

        // =========================
        // STEP 4: BUILD ROWS
        // =========================
        List<Map<String, Object>> rows = new ArrayList<>();

        for (Document doc : output) {

            String key = doc.getString("log_date") + "_" + doc.getInteger("status_code");
            Set<Integer> batches = batchTracker.get(key);
            List<Integer> sortedBatches = new ArrayList<>(batches);
            Collections.sort(sortedBatches);

            String batchString = String.join("+",
                    sortedBatches.stream().map(String::valueOf).toArray(String[]::new));

            java.sql.Date sqlDate = java.sql.Date.valueOf(doc.getString("log_date"));

            Map<String, Object> row = new LinkedHashMap<>();
            row.put("log_date",      sqlDate);
            row.put("status_code",   doc.getInteger("status_code"));
            row.put("request_count", doc.getInteger("request_count"));
            row.put("total_bytes",   doc.getLong("total_bytes"));
            row.put("batch_id",      batchString);
            rows.add(row);
        }

        System.out.println("Rows to insert: " + rows.size());

        // =========================
        // STEP 5: INSERT INTO POSTGRES
        // =========================
        PostgresInsertService.Insert("pig", "query_1", rows);

        // =========================
        // STEP 6: PRINT OUTPUT
        // =========================
        System.out.printf("%-12s | %-12s | %-15s | %-15s | %-10s%n",
                "log_date", "status_code", "request_count", "total_bytes", "batches");
        System.out.println("--------------------------------------------------------------------------------------------------------------");

        for (Map<String, Object> row : rows) {
            System.out.printf("%-12s | %-12d | %-15d | %-15d | %-10s%n",
                    row.get("log_date"),
                    row.get("status_code"),
                    row.get("request_count"),
                    row.get("total_bytes"),
                    row.get("batch_id"));
        }
    }
}