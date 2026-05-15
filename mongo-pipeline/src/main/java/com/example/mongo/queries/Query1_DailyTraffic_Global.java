package com.example.mongo.queries;

import com.example.postgres.service.PostgresInsertService;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

public class Query1_DailyTraffic_Global {

    public static void run(MongoDatabase database) {

        String runId = UUID.randomUUID().toString();
        java.sql.Timestamp executedAt = java.sql.Timestamp.from(Instant.now());

        List<String> batchCollections = new ArrayList<>();

        for (String name : database.listCollectionNames()) {
            if (name.startsWith("filtered_logs_batch_")) {
                batchCollections.add(name);
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
                                                .append("request_count", new Document("$sum", 1))
                                                .append("total_bytes", new Document("$sum", "$bytes"))
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

        // =========================
        // STEP 2: MERGE RESULTS
        // =========================
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
            }

        } catch (Exception e) {
            e.printStackTrace();
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

            // Convert "01/Jul/1995" → "1995-07-01"
            String formattedDate = convertDate(doc.getString("log_date"));
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
        PostgresInsertService.Insert( // runid
                "mongodb",
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

    // Convert "01/Jul/1995" → "1995-07-01"
    private static String convertDate(String input) {
        try {
            DateTimeFormatter inputFmt =
                    DateTimeFormatter.ofPattern("dd/MMM/yyyy", Locale.ENGLISH);

            DateTimeFormatter outputFmt =
                    DateTimeFormatter.ofPattern("yyyy-MM-dd");

            return LocalDate.parse(input, inputFmt).format(outputFmt);

        } catch (Exception e) {
            return input;
        }
    }
}