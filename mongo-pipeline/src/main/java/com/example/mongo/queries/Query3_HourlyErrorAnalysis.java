package com.example.mongo.queries;

import com.example.postgres.service.PostgresInsertService;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public class Query3_HourlyErrorAnalysis {

    public static void run(MongoDatabase database) { // runid

        // metadata
        String pipelineName = "mongodb";
        String runId = UUID.randomUUID().toString();
        java.sql.Timestamp executedAt =
                java.sql.Timestamp.from(Instant.now());

        // Get all batch collections
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
        Map<String, Document> finalMap =
                new HashMap<>();

        // Track contributing batches
        Map<String, Set<Integer>> batchTracker =
                new HashMap<>();

        // Run same query on each batch collection in parallel
        for (String collectionName : batchCollections) {

            futures.add(executor.submit(() -> {

                MongoCollection<Document> collection =
                        database.getCollection(collectionName);

                int batchId = Integer.parseInt(
                        collectionName.substring(
                                collectionName.lastIndexOf("_") + 1
                        )
                );

                AggregateIterable<Document> result =
                        collection.aggregate(Arrays.asList(

                                new Document("$group",
                                        new Document("_id",
                                                new Document("log_date", "$date")
                                                        .append("log_hour", "$hour")
                                        )

                                                .append("error_request_count",
                                                        new Document("$sum",
                                                                new Document("$cond", Arrays.asList(
                                                                        new Document("$and", Arrays.asList(
                                                                                new Document("$gte", Arrays.asList("$status", 400)),
                                                                                new Document("$lte", Arrays.asList("$status", 599))
                                                                        )),
                                                                        1,
                                                                        0
                                                                ))
                                                        )
                                                )

                                                .append("total_request_count",
                                                        new Document("$sum", 1)
                                                )

                                                .append("distinct_error_hosts",
                                                        new Document("$addToSet",
                                                                new Document("$cond", Arrays.asList(
                                                                        new Document("$and", Arrays.asList(
                                                                                new Document("$gte", Arrays.asList("$status", 400)),
                                                                                new Document("$lte", Arrays.asList("$status", 599))
                                                                        )),
                                                                        "$host",
                                                                        "$$REMOVE"
                                                                ))
                                                        )
                                                )
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

        // Merge outputs of all batches
        try {

            for (Future<List<Document>> future : futures) {

                List<Document> partialResults =
                        future.get();

                for (Document doc : partialResults) {

                    Document id =
                            (Document) doc.get("_id");

                    String date =
                            id.getString("log_date");

                    int hour =
                            id.getInteger("log_hour");

                    int batchId =
                            doc.getInteger("batch_id");

                    String key =
                            date + "_" + hour;

                    int errors =
                            doc.getInteger("error_request_count");

                    int total =
                            doc.getInteger("total_request_count");

                    List<String> hosts =
                            (List<String>) doc.get("distinct_error_hosts");

                    if (!finalMap.containsKey(key)) {

                        finalMap.put(key,
                                new Document("log_date", date)
                                        .append("log_hour", hour)
                                        .append("error_request_count", errors)
                                        .append("total_request_count", total)
                                        .append("hosts", new HashSet<>(hosts))
                        );

                        batchTracker.put(key, new HashSet<>());

                    } else {

                        Document existing =
                                finalMap.get(key);

                        existing.put("error_request_count",
                                existing.getInteger("error_request_count") + errors);

                        existing.put("total_request_count",
                                existing.getInteger("total_request_count") + total);

                        Set<String> hostSet =
                                (Set<String>) existing.get("hosts");

                        hostSet.addAll(hosts);
                    }

                    batchTracker.get(key).add(batchId);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        // Sort final result
        List<Document> output =
                new ArrayList<>(finalMap.values());

        output.sort(
                Comparator.comparing(
                                (Document d) -> d.getString("log_date"))
                        .thenComparing(
                                d -> d.getInteger("log_hour"))
        );


        List<Map<String, Object>> rows =
                new ArrayList<>();

        for (Document doc : output) {

            int errors =
                    doc.getInteger("error_request_count");

            int total =
                    doc.getInteger("total_request_count");

            double errorRate =
                    total == 0 ? 0 :
                            Math.round((errors * 10000.0 / total)) / 100.0;

            String key =
                    doc.getString("log_date") + "_" +
                            doc.getInteger("log_hour");

            Set<Integer> batches =
                    batchTracker.get(key);

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

            row.put("log_date",
                    doc.getString("log_date"));

            row.put("log_hour",
                    doc.getInteger("log_hour"));

            row.put("error_request_count",
                    errors);

            row.put("total_request_count",
                    total);

            row.put("error_rate",
                    errorRate);

            row.put("distinct_error_hosts",
                    ((Set<?>) doc.get("hosts")).size());

            row.put("batch_id",
                    batchString);

            row.put("run_id",
                    runId);

            row.put("pipeline_name",
                    pipelineName);

            row.put("executed_at",
                    executedAt);

            rows.add(row);
        }

        // INSERT INTO POSTGRES TABLE query_3
        PostgresInsertService.Insert(
                "mongodb",
                "query_3",
                rows
        );

        // =========================================
        // PRINT OUTPUT
        // =========================================

        System.out.printf(
                "%-12s | %-8s | %-20s | %-20s | %-12s | %-20s | %-10s | %-36s | %-10s | %-25s%n",
                "log_date", "log_hour", "error_request_count",
                "total_request_count", "error_rate",
                "distinct_error_hosts", "batches",
                "run_id", "pipeline", "executed_at"
        );

        System.out.println("--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");

        for (Map<String, Object> row : rows) {

            System.out.printf(
                    "%-12s | %-8d | %-20d | %-20d | %-12.2f | %-20d | %-10s | %-36s | %-10s | %-25s%n",
                    row.get("log_date"),
                    row.get("log_hour"),
                    row.get("error_request_count"),
                    row.get("total_request_count"),
                    row.get("error_rate"),
                    row.get("distinct_error_hosts"),
                    row.get("batch_id"),
                    row.get("run_id"),
                    row.get("pipeline_name"),
                    row.get("executed_at")
            );
        }
    }
}