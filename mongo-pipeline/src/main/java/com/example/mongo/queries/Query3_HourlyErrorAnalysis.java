package com.example.mongo.queries;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.*;

public class Query3_HourlyErrorAnalysis {

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

        // Step 2: Run same query on each batch collection in parallel
        for (String collectionName : batchCollections) {

            futures.add(executor.submit(() -> {

                MongoCollection<Document> collection =
                        database.getCollection(collectionName);

                AggregateIterable<Document> result =
                        collection.aggregate(Arrays.asList(

                                // group by date + hour
                                new Document("$group",
                                        new Document("_id",
                                                new Document("date", "$date")
                                                        .append("hour", "$hour")
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
                    docs.add(doc);
                }

                return docs;
            }));
        }

        executor.shutdown();

        // Step 3: Merge outputs of all batches
        Map<String, Document> finalMap =
                new HashMap<>();

        try {

            for (Future<List<Document>> future : futures) {

                List<Document> partialResults =
                        future.get();

                for (Document doc : partialResults) {

                    Document id =
                            (Document) doc.get("_id");

                    String date =
                            id.getString("date");

                    int hour =
                            id.getInteger("hour");

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
                                new Document("logdate", date)
                                        .append("loghour", hour)
                                        .append("errors", errors)
                                        .append("total", total)
                                        .append("hosts",
                                                new HashSet<>(hosts))
                        );

                    } else {

                        Document existing =
                                finalMap.get(key);

                        existing.put("errors",
                                existing.getInteger("errors") + errors);

                        existing.put("total",
                                existing.getInteger("total") + total);

                        Set<String> hostSet =
                                (Set<String>) existing.get("hosts");

                        hostSet.addAll(hosts);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        // Step 4: Sort + print final result
        List<Document> output =
                new ArrayList<>(finalMap.values());

        output.sort(
                Comparator.comparing(
                                (Document d) -> d.getString("logdate"))
                        .thenComparing(
                                d -> d.getInteger("loghour"))
        );

        for (Document doc : output) {

            int errors =
                    doc.getInteger("errors");

            int total =
                    doc.getInteger("total");

            double errorRate =
                    total == 0 ? 0 :
                            Math.round(
                                    (errors * 10000.0 / total)
                            ) / 100.0;

            System.out.println(
                    new Document("logdate",
                            doc.getString("logdate"))
                            .append("loghour",
                                    doc.getInteger("loghour"))
                            .append("errorrequestcount",
                                    errors)
                            .append("totalrequestcount",
                                    total)
                            .append("errorrate",
                                    errorRate)
                            .append("distincterrorhosts",
                                    ((Set<?>) doc.get("hosts")).size())
                            .toJson()
            );
        }
    }
}