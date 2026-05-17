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
 * Hourly error-rate analysis per (date, hour) across all valid batches.
 *
 * <h3>What changed vs the original</h3>
 * <ul>
 *   <li>Each {@code Callable} calls {@link PigServerManager#close()} in a
 *       {@code finally} block to release the thread-local {@link org.apache.pig.PigServer}.
 *   <li>{@link PigScriptExecutor#executeQueryScript} now uses the embedded Pig
 *       API instead of a {@code ProcessBuilder} subprocess.
 * </ul>
 *
 * All merging, sorting, error-rate computation, Postgres insertion, and console
 * output logic is identical to the original.
 */
public class Query3_HourlyErrorAnalysis {

    private static final String SCRIPT_PATH =
            "Pig-pipeline/src/main/resources/pig/query3.pig";

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
                    String outputDir = "./pig_data/query3_out/" + batchDirName;

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
                            if (parts.length >= 6 && !parts[2].isEmpty()) {
                                Document doc = new Document();
                                doc.append("_id", new Document("log_date", parts[0])
                                        .append("log_hour", Integer.parseInt(parts[1])));
                                doc.append("error_request_count",  Integer.parseInt(parts[2]));
                                doc.append("total_request_count",  Integer.parseInt(parts[3]));
                                doc.append("distinct_error_hosts", PigBagParser.parseBag(parts[4]));
                                doc.append("batch_id",             Integer.parseInt(parts[5]));
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

                    Document     id      = (Document) doc.get("_id");
                    String       date    = id.getString("log_date");
                    int          hour    = id.getInteger("log_hour");
                    int          batchId = doc.getInteger("batch_id");
                    String       key     = date + "_" + hour;

                    int          errors  = doc.getInteger("error_request_count");
                    int          total   = doc.getInteger("total_request_count");
                    List<String> hosts   = (List<String>) doc.get("distinct_error_hosts");

                    if (!finalMap.containsKey(key)) {
                        finalMap.put(key,
                                new Document("log_date",             date)
                                        .append("log_hour",              hour)
                                        .append("error_request_count",   errors)
                                        .append("total_request_count",   total)
                                        .append("hosts",                 new HashSet<>(hosts)));
                        batchTracker.put(key, new HashSet<>());
                    } else {
                        Document existing = finalMap.get(key);
                        existing.put("error_request_count", existing.getInteger("error_request_count") + errors);
                        existing.put("total_request_count", existing.getInteger("total_request_count") + total);
                        ((Set<String>) existing.get("hosts")).addAll(hosts);
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
        output.sort(Comparator.comparing((Document d) -> d.getString("log_date"))
                .thenComparing(d -> d.getInteger("log_hour")));

        // =========================
        // STEP 4: BUILD ROWS
        // =========================
        List<Map<String, Object>> rows = new ArrayList<>();

        for (Document doc : output) {

            int errors = doc.getInteger("error_request_count");
            int total  = doc.getInteger("total_request_count");

            double errorRate = total == 0 ? 0
                    : Math.round((errors * 10000.0 / total)) / 100.0;

            String key = doc.getString("log_date") + "_" + doc.getInteger("log_hour");
            Set<Integer> batches = batchTracker.get(key);
            List<Integer> sortedBatches = new ArrayList<>(batches);
            Collections.sort(sortedBatches);

            String batchString = String.join("+",
                    sortedBatches.stream().map(String::valueOf).toArray(String[]::new));

            Map<String, Object> row = new LinkedHashMap<>();
            row.put("log_date",             doc.getString("log_date"));
            row.put("log_hour",             doc.getInteger("log_hour"));
            row.put("error_request_count",  errors);
            row.put("total_request_count",  total);
            row.put("error_rate",           errorRate);
            row.put("distinct_error_hosts", ((Set<?>) doc.get("hosts")).size());
            row.put("batch_id",             batchString);
            rows.add(row);
        }

        // =========================
        // STEP 5: INSERT INTO POSTGRES
        // =========================
        PostgresInsertService.Insert("pig", "query_3", rows);

        // =========================
        // STEP 6: PRINT OUTPUT
        // =========================
        System.out.printf("%-12s | %-10s | %-20s | %-20s | %-12s | %-20s | %-10s%n",
                "log_date", "log_hour", "error_request_count", "total_request_count",
                "error_rate", "distinct_error_hosts", "batches");
        System.out.println("----------------------------------------------------------------------------------------------------------------------------------");

        for (Map<String, Object> row : rows) {
            System.out.printf("%-12s | %-10d | %-20d | %-20d | %-12.2f | %-20d | %-10s%n",
                    row.get("log_date"),
                    row.get("log_hour"),
                    row.get("error_request_count"),
                    row.get("total_request_count"),
                    row.get("error_rate"),
                    row.get("distinct_error_hosts"),
                    row.get("batch_id"));
        }
    }
}