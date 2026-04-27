package com.example.mongo.queries;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.*;

public class Query1_DailyTraffic_Global {

    public static void run(MongoDatabase database) {

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

        // Step 2: Run aggregation per batch (parallel)
        for (String collectionName : batchCollections) {

            futures.add(executor.submit(() -> {

                MongoCollection<Document> collection =
                        database.getCollection(collectionName);

                AggregateIterable<Document> result =
                        collection.aggregate(Arrays.asList(

                                // GROUP
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
                    docs.add(doc);
                }

                return docs;
            }));
        }

        executor.shutdown();

        // Step 3: Merge all batches → GLOBAL aggregation
        try {

            for (Future<List<Document>> future : futures) {

                List<Document> partialResults = future.get();

                for (Document doc : partialResults) {

                    Document id = (Document) doc.get("_id");

                    String date = id.getString("log_date");
                    int status = id.getInteger("status_code");

                    String key = date + "_" + status;

                    int count = doc.getInteger("request_count");
                    long bytes = doc.getLong("total_bytes");

                    if (!globalMap.containsKey(key)) {

                        globalMap.put(key,
                                new Document("log_date", date)
                                        .append("status_code", status)
                                        .append("request_count", count)
                                        .append("total_bytes", bytes)
                        );

                    } else {

                        Document existing = globalMap.get(key);

                        existing.put("request_count",
                                existing.getInteger("request_count") + count);

                        existing.put("total_bytes",
                                existing.getLong("total_bytes") + bytes);
                    }
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

        // Step 5: Print formatted output

        System.out.println("\n===== QUERY 1: GLOBAL DAILY TRAFFIC =====\n");

        System.out.printf(
                "%-12s | %-12s | %-15s | %-15s%n",
                "log_date", "status_code", "request_count", "total_bytes"
        );

        System.out.println("---------------------------------------------------------------");

        for (Document doc : output) {

            System.out.printf(
                    "%-12s | %-12d | %-15d | %-15d%n",
                    doc.getString("log_date"),
                    doc.getInteger("status_code"),
                    doc.getInteger("request_count"),
                    doc.getLong("total_bytes")
            );
        }
    }
}