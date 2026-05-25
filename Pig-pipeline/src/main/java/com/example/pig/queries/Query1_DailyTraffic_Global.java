package com.example.pig.queries;

import com.example.pig.util.PigScriptExecutor;
import com.example.pig.util.PigServerManager;
import com.example.postgres.service.PostgresInsertService;
import org.bson.Document;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;


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


        int numThreads = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        List<Future<List<Document>>> futures = new ArrayList<>();

        Map<String, Document>    finalMap     = new HashMap<>();
        Map<String, Set<Integer>> batchTracker = new HashMap<>();

        // per-batch query
        for (String batchDirName : batchCollections) {

            futures.add(executor.submit(() -> {
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

        // Wait for all batch tasks to complete and gather results
        List<List<Document>> resultsList = new ArrayList<>();
        for (Future<List<Document>> future : futures) {
            try {
                resultsList.add(future.get());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Clean up thread-local PigServers in the pool
        List<Future<?>> cleanupFutures = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            cleanupFutures.add(executor.submit(() -> {
                PigServerManager.close();
                return null;
            }));
        }
        for (Future<?> f : cleanupFutures) {
            try {
                f.get(1, TimeUnit.SECONDS);
            } catch (Exception e) {
                // ignore
            }
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Cleanly shut down the PigServer that was used by this (main) thread.
        PigServerManager.close();

        // Merging the results
        for (List<Document> partialResults : resultsList) {
            try {
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


        List<Document> output = new ArrayList<>(finalMap.values());
        output.sort(
                Comparator.comparing((Document d) -> d.getString("log_date"))
                        .thenComparing(d -> d.getInteger("status_code")));

        // Building the rows
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

        // Postgres insertion
        PostgresInsertService.Insert("pig", "query_1", rows);


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