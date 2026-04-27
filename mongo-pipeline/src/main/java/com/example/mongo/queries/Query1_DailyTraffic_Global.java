package com.example.mongo.queries;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public class Query1_DailyTraffic_Global {

    public static void run(MongoDatabase database) {

        // 🔹 metadata
        String pipelineName = "mongodb";
        String runId = UUID.randomUUID().toString();
        String executedAt = Instant.now().toString();

        // Step 1: Get all batch collections
        List<String> batchCollections = new ArrayList<>();

        for (String name : database.listCollectionNames()) {
            if (name.startsWith("filtered_logs_batch_")) {
                batchCollections.add(name);
            }
        }

        int threads = Runtime.getRuntime().availableProcessors();

        ExecutorService executor =
                Executors.newFixedThreadPool(threads);

        List<Future<List<Document>>> futures =
                new ArrayList<>();

        // GLOBAL aggregation map
        Map<String, Document> globalMap = new HashMap<>();

        // Track contributing batch IDs
        Map<String, Set<Integer>> batchTracker = new HashMap<>();

        // Step 2: Run per-batch aggregation
        for (String collectionName : batchCollections) {

            futures.add(executor.submit(() -> {

                MongoCollection<Document> collection =
                        database.getCollection(collectionName);

                int batchId = Integer.parseInt(
                        collectionName.substring(collectionName.lastIndexOf("_") + 1)
                );

                AggregateIterable<Document> result =
                        collection.aggregate(Arrays.asList(

                                new Document("$group",
                                        new Document("_id",
                                                new Document("log_date", "$date")
                                                        .append("status_code", "$status")
                                        )
                                                .append("request_count",
                                                        new Document("$sum", 1))
                                                .append("total_bytes",
                                                        new Document("$sum", "$bytes"))
                                )
                        ));

                List<Document> docs = new ArrayList<>();

                for (Document doc : result) {

                    doc.append("batch_id", batchId);
                    docs.add(doc);
                }

                return docs;
            }));
        }

        executor.shutdown();

        // Step 3: Merge → GLOBAL aggregation + track batches
        try {

            for (Future<List<Document>> future : futures) {

                List<Document> partialResults = future.get();

                for (Document doc : partialResults) {

                    Document id = (Document) doc.get("_id");

                    String date = id.getString("log_date");
                    int status = id.getInteger("status_code");
                    int batchId = doc.getInteger("batch_id");

                    String key = date + "_" + status;

                    int count = doc.getInteger("request_count");
                    long bytes = doc.getLong("total_bytes");

                    // aggregate values
                    if (!globalMap.containsKey(key)) {

                        globalMap.put(key,
                                new Document("log_date", date)
                                        .append("status_code", status)
                                        .append("request_count", count)
                                        .append("total_bytes", bytes)
                        );

                        batchTracker.put(key, new HashSet<>());

                    } else {

                        Document existing = globalMap.get(key);

                        existing.put("request_count",
                                existing.getInteger("request_count") + count);

                        existing.put("total_bytes",
                                existing.getLong("total_bytes") + bytes);
                    }

                    // track batch contribution
                    batchTracker.get(key).add(batchId);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        // Step 4: Sort results
        List<Document> output =
                new ArrayList<>(globalMap.values());

        output.sort(
                Comparator.comparing((Document d) -> d.getString("log_date"))
                        .thenComparing(d -> d.getInteger("status_code"))
        );

        // Step 5: Print FINAL output with metadata

        System.out.println("\n===== QUERY 1: GLOBAL DAILY TRAFFIC =====\n");

        System.out.printf(
                "%-12s | %-12s | %-15s | %-15s | %-10s | %-36s | %-10s | %-25s%n",
                "log_date", "status_code", "request_count", "total_bytes",
                "batches", "run_id", "pipeline", "executed_at"
        );

        System.out.println("--------------------------------------------------------------------------------------------------------------");

        for (Document doc : output) {

            String key =
                    doc.getString("log_date") + "_" +
                            doc.getInteger("status_code");

            // convert batch set → "1+2+3"
            Set<Integer> batches = batchTracker.get(key);

            List<Integer> sortedBatches = new ArrayList<>(batches);
            Collections.sort(sortedBatches);

            String batchString =
                    String.join("+",
                            sortedBatches.stream()
                                    .map(String::valueOf)
                                    .toArray(String[]::new)
                    );

            System.out.printf(
                    "%-12s | %-12d | %-15d | %-15d | %-10s | %-36s | %-10s | %-25s%n",
                    doc.getString("log_date"),
                    doc.getInteger("status_code"),
                    doc.getInteger("request_count"),
                    doc.getLong("total_bytes"),
                    batchString,
                    runId,
                    pipelineName,
                    executedAt
            );
        }
    }
}