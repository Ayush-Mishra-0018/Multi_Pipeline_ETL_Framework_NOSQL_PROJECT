package com.example.mongo.queries;

import com.example.mongo.service.MongoConnection;
import com.example.postgres.service.PostgresInsertService;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public class Query2_TopResources {

    public static void run(MongoDatabase database) {

        // metadata
        String pipelineName = "mongodb";
        String runId = UUID.randomUUID().toString();
        java.sql.Timestamp executedAt = java.sql.Timestamp.from(Instant.now());

        // Step 1: collect all batch collections
        List<String> batchCollections = new ArrayList<>();

        for (String name : database.listCollectionNames()) {
            if (name.startsWith("filtered_logs_batch_")) {
                batchCollections.add(name);
            }
        }

        // fallback if batch collections do not exist
        if (batchCollections.isEmpty()) {
            batchCollections.add("filtered_logs");
        }

        batchCollections.sort(Comparator.naturalOrder());

        int threads = Math.min(
                batchCollections.size(),
                Runtime.getRuntime().availableProcessors()
        );

        ExecutorService executor =
                Executors.newFixedThreadPool(threads);

        List<Future<List<Document>>> futures =
                new ArrayList<>();

        // final merged map
        Map<String, Document> finalMap =
                new HashMap<>();

        // track contributing batches
        Map<String, Set<Integer>> batchTracker =
                new HashMap<>();

        // Step 2: run local aggregation in parallel
        for (String collectionName : batchCollections) {

            futures.add(executor.submit(() -> {

                MongoCollection<Document> collection =
                        database.getCollection(collectionName);

                int batchId = 0;

                if (collectionName.startsWith("filtered_logs_batch_")) {
                    batchId = Integer.parseInt(
                            collectionName.substring(
                                    collectionName.lastIndexOf("_") + 1
                            )
                    );
                }

                AggregateIterable<Document> result =
                        collection.aggregate(Arrays.asList(

                                new Document("$match",
                                        new Document("path",
                                                new Document("$ne", "/"))
                                ),

                                new Document("$group",
                                        new Document("_id", "$path")
                                                .append("requestCount",
                                                        new Document("$sum", 1))
                                                .append("totalBytes",
                                                        new Document("$sum", "$bytes"))
                                                .append("hosts",
                                                        new Document("$addToSet", "$host"))
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

        try {
            executor.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Step 3: merge partial outputs
        try {

            for (Future<List<Document>> future : futures) {

                List<Document> partialResults =
                        future.get();

                for (Document doc : partialResults) {

                    String path =
                            doc.getString("_id");

                    int requestCount =
                            doc.getInteger("requestCount");

                    long totalBytes =
                            ((Number) doc.get("totalBytes")).longValue();

                    List<String> hosts =
                            (List<String>) doc.get("hosts");

                    int batchId =
                            doc.getInteger("batch_id");

                    if (!finalMap.containsKey(path)) {

                        finalMap.put(path,
                                new Document("resource_path", path)
                                        .append("request_count", requestCount)
                                        .append("total_bytes", totalBytes)
                                        .append("hosts", new HashSet<>(hosts))
                        );

                        batchTracker.put(path, new HashSet<>());

                    } else {

                        Document existing =
                                finalMap.get(path);

                        existing.put("request_count",
                                existing.getInteger("request_count")
                                        + requestCount);

                        existing.put("total_bytes",
                                ((Number) existing.get("total_bytes")).longValue()
                                        + totalBytes);

                        Set<String> hostSet =
                                (Set<String>) existing.get("hosts");

                        hostSet.addAll(hosts);
                    }

                    batchTracker.get(path).add(batchId);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        // Step 4: top 20 globally
        List<Document> output =
                new ArrayList<>(finalMap.values());

        output.sort(
                Comparator.comparing(
                                (Document d) -> d.getInteger("request_count"))
                        .reversed()
                        .thenComparing(d -> d.getString("resource_path"))
        );

        if (output.size() > 20) {
            output = new ArrayList<>(output.subList(0, 20));
        }

        // display ascending like expected sheet
        output.sort(
                Comparator.comparing(
                                (Document d) -> d.getInteger("request_count"))
                        .thenComparing(d -> d.getString("resource_path"))
        );

        // Step 5: prepare rows for postgres
        List<Map<String, Object>> rows =
                new ArrayList<>();

        for (Document doc : output) {

            String path =
                    doc.getString("resource_path");

            Set<Integer> batches =
                    batchTracker.get(path);

            List<Integer> sortedBatches =
                    new ArrayList<>(batches);

            Collections.sort(sortedBatches);

            String batchString =
                    String.join("+",
                            sortedBatches.stream()
                                    .map(String::valueOf)
                                    .toArray(String[]::new)
                    );

            Map<String, Object> row =
                    new LinkedHashMap<>();

            row.put("resource_path", path);
            row.put("request_count",
                    doc.getInteger("request_count"));
            row.put("total_bytes",
                    ((Number) doc.get("total_bytes")).longValue());
            row.put("distinct_hosts",
                    ((Set<?>) doc.get("hosts")).size());
            row.put("batch_id", batchString);

            rows.add(row);
        }

        // INSERT INTO POSTGRES query_2
        PostgresInsertService.Insert(
                "mongodb",
                "query_2",
                rows
        );

        // Step 6: print
        System.out.printf(
                "%-50s | %-14s | %-14s | %-15s | %-10s%n",
                "resource_path", "request_count", "total_bytes",
                "distinct_hosts", "batches", "run_id",
                "pipeline", "executed_at"
        );

        System.out.println("-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");

        for (Map<String, Object> row : rows) {

            System.out.printf(
                    "%-50s | %-14d | %-14d | %-15d | %-10s%n",
                    row.get("resource_path"),
                    row.get("request_count"),
                    row.get("total_bytes"),
                    row.get("distinct_hosts"),
                    row.get("batch_id")
            );
        }
    }

    public static void main(String[] args) {
        try {
            run(MongoConnection.getDatabase());
        } finally {
            MongoConnection.close();
        }
    }
}