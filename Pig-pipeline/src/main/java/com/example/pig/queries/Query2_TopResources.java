package com.example.pig.queries;

import com.example.pig.util.PigBagParser;
import com.example.pig.util.PigScriptExecutor;
import com.example.pig.util.PigServerManager;
import com.example.postgres.service.PostgresInsertService;
import org.bson.Document;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

/**
 * Top-20 most-requested resources across all valid batches.
 *
 * <h3>What changed vs the original</h3>
 * <ul>
 *   <li>Each {@code Callable} calls {@link PigServerManager#close()} in a
 *       {@code finally} block to release the thread-local {@link org.apache.pig.PigServer}.
 *   <li>{@link PigScriptExecutor#executeQueryScript} now uses the embedded Pig
 *       API instead of a {@code ProcessBuilder} subprocess.
 * </ul>
 *
 * All merging, top-20 selection, Postgres insertion, and console output logic
 * is identical to the original.
 */
public class Query2_TopResources {

    private static final String SCRIPT_PATH =
            "Pig-pipeline/src/main/resources/pig/query2.pig";

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

        ExecutorService executor =
                Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        List<Future<List<Document>>> futures = new ArrayList<>();

        Map<String, Document>    finalMap     = new HashMap<>();
        Map<String, Set<Integer>> batchTracker = new HashMap<>();

        // =========================
        // STEP 1: PER-BATCH QUERY
        // =========================
        for (String batchDirName : batchCollections) {

            futures.add(executor.submit(() -> {
                try {
                    int batchId = Integer.parseInt(
                            batchDirName.substring(batchDirName.lastIndexOf("_") + 1));

                    String inputDir  = "./pig_data/valid/"     + batchDirName;
                    String outputDir = "./pig_data/query2_out/" + batchDirName;

                    PigScriptExecutor.deleteDirectory(new File(outputDir));

                    // ── KEY CHANGE ────────────────────────────────────────────
                    // Original: ProcessBuilder subprocess for each batch.
                    // New:      embedded PigServer (thread-local, reused across
                    //           batches on the same thread).
                    // ─────────────────────────────────────────────────────────
                    PigScriptExecutor.executeQueryScript(SCRIPT_PATH, inputDir, outputDir, batchId);

                    List<String> lines = PigScriptExecutor.readOutputLines(outputDir);

                    List<Document> docs = new ArrayList<>();
                    for (String line : lines) {
                        try {
                            String[] parts = line.split("\t");
                            if (parts.length >= 5 && !parts[1].isEmpty()) {
                                Document doc = new Document();
                                doc.append("_id",          parts[0]);
                                doc.append("requestCount", Integer.parseInt(parts[1]));
                                doc.append("totalBytes",   Long.parseLong(parts[2]));
                                doc.append("hosts",        PigBagParser.parseBag(parts[3]));
                                doc.append("batch_id",     Integer.parseInt(parts[4]));
                                docs.add(doc);
                            }
                        } catch (Exception e) {
                            System.err.println("Skipping malformed line: " + line);
                        }
                    }

                    PigScriptExecutor.deleteDirectory(new File(outputDir));
                    return docs;

                } finally {
                    PigServerManager.close();
                }
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

                    String       path         = doc.getString("_id");
                    int          requestCount = doc.getInteger("requestCount");
                    long         totalBytes   = doc.getLong("totalBytes");
                    List<String> hosts        = (List<String>) doc.get("hosts");
                    int          batchId      = doc.getInteger("batch_id");

                    if (!finalMap.containsKey(path)) {
                        finalMap.put(path,
                                new Document("resource_path",  path)
                                        .append("request_count", requestCount)
                                        .append("total_bytes",   totalBytes)
                                        .append("hosts",         new HashSet<>(hosts)));
                        batchTracker.put(path, new HashSet<>());
                    } else {
                        Document existing = finalMap.get(path);
                        existing.put("request_count", existing.getInteger("request_count") + requestCount);
                        existing.put("total_bytes",   existing.getLong("total_bytes")   + totalBytes);
                        ((Set<String>) existing.get("hosts")).addAll(hosts);
                    }

                    batchTracker.get(path).add(batchId);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // =========================
        // STEP 3: TOP 20
        // =========================
        List<Document> output = new ArrayList<>(finalMap.values());
        output.sort(Comparator.comparing((Document d) -> d.getInteger("request_count"))
                .reversed().thenComparing(d -> d.getString("resource_path")));

        if (output.size() > 20) output = new ArrayList<>(output.subList(0, 20));

        output.sort(Comparator.comparing((Document d) -> d.getInteger("request_count"))
                .thenComparing(d -> d.getString("resource_path")));

        // =========================
        // STEP 4: BUILD ROWS
        // =========================
        List<Map<String, Object>> rows = new ArrayList<>();

        for (Document doc : output) {

            String path = doc.getString("resource_path");
            Set<Integer> batches = batchTracker.get(path);
            List<Integer> sortedBatches = new ArrayList<>(batches);
            Collections.sort(sortedBatches);

            String batchString = String.join("+",
                    sortedBatches.stream().map(String::valueOf).toArray(String[]::new));

            Map<String, Object> row = new LinkedHashMap<>();
            row.put("resource_path",  path);
            row.put("request_count",  doc.getInteger("request_count"));
            row.put("total_bytes",    doc.getLong("total_bytes"));
            row.put("distinct_hosts", ((Set<?>) doc.get("hosts")).size());
            row.put("batch_id",       batchString);
            rows.add(row);
        }

        // =========================
        // STEP 5: INSERT INTO POSTGRES
        // =========================
        PostgresInsertService.Insert("pig", "query_2", rows);

        // =========================
        // STEP 6: PRINT OUTPUT
        // =========================
        System.out.printf("%-50s | %-14s | %-14s | %-15s | %-10s%n",
                "resource_path", "request_count", "total_bytes", "distinct_hosts", "batches");
        System.out.println("-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");

        for (Map<String, Object> row : rows) {
            System.out.printf("%-50s | %-14d | %-14d | %-15d | %-10s%n",
                    row.get("resource_path"),
                    row.get("request_count"),
                    row.get("total_bytes"),
                    row.get("distinct_hosts"),
                    row.get("batch_id"));
        }
    }
}