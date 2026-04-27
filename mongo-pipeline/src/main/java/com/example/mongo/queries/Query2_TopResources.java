package com.example.mongo.queries;

import com.example.mongo.service.MongoConnection;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.*;

public class Query2_TopResources {

    public static void run(MongoDatabase database) {

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

        // sort batch names for deterministic execution
        batchCollections.sort(Comparator.naturalOrder());

        int threads = Math.min(
                batchCollections.size(),
                Runtime.getRuntime().availableProcessors()
        );

        ExecutorService executor =
                Executors.newFixedThreadPool(threads);

        List<Future<List<Document>>> futures =
                new ArrayList<>();

        // Step 2: run local aggregation in parallel
        for (String collectionName : batchCollections) {

            futures.add(executor.submit(() -> {

                MongoCollection<Document> collection =
                        database.getCollection(collectionName);

                AggregateIterable<Document> result =
                        collection.aggregate(Arrays.asList(

                                // exclude only root path
                                new Document("$match",
                                        new Document("path",
                                                new Document("$ne", "/"))
                                ),

                                // local grouping
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
        Map<String, Document> finalMap =
                new HashMap<>();

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

                    if (!finalMap.containsKey(path)) {

                        finalMap.put(path,
                                new Document("path", path)
                                        .append("requestCount", requestCount)
                                        .append("totalBytes", totalBytes)
                                        .append("hosts",
                                                new HashSet<>(hosts))
                        );

                    } else {

                        Document existing =
                                finalMap.get(path);

                        existing.put("requestCount",
                                existing.getInteger("requestCount")
                                        + requestCount);

                        existing.put("totalBytes",
                                ((Number) existing.get("totalBytes")).longValue()
                                        + totalBytes);

                        Set<String> hostSet =
                                (Set<String>) existing.get("hosts");

                        hostSet.addAll(hosts);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        // Step 4: global top 20
        List<Document> output =
                new ArrayList<>(finalMap.values());

        // select top 20 by descending count
        output.sort(
                Comparator.comparing(
                                (Document d) -> d.getInteger("requestCount"))
                        .reversed()
                        .thenComparing(d -> d.getString("path"))
        );

        if (output.size() > 20) {
            output = new ArrayList<>(output.subList(0, 20));
        }

        // display ascending like reference output
        output.sort(
                Comparator.comparing(
                                (Document d) -> d.getInteger("requestCount"))
                        .thenComparing(d -> d.getString("path"))
        );

        // Step 5: print
        System.out.println("\n========== QUERY 2 : Top Requested Resources ==========");

        for (Document doc : output) {

            System.out.println(
                    doc.getString("path") + " | " +
                            doc.getInteger("requestCount") + " | " +
                            ((Number) doc.get("totalBytes")).longValue() + " | " +
                            ((Set<?>) doc.get("hosts")).size()
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