package com.example.mongo.queries;

import com.example.mongo.service.MongoConnection;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Arrays;

public final class Query2_TopResources {

    private Query2_TopResources() {}

    public static void run() {

        MongoDatabase db = MongoConnection.getDatabase();
        MongoCollection<Document> logs =
                db.getCollection("filtered_logs");

        System.out.println("\n========== QUERY 2 : Top Requested Resources ==========");

        AggregateIterable<Document> result =
                logs.aggregate(Arrays.asList(

                        // keep file-like paths only
                        new Document("$match",
                                new Document("path",
                                        new Document("$ne", "/")
                                                .append("$not",
                                                        new Document("$regex", "/$"))
                                )
                        ),

                        // aggregate
                        new Document("$group",
                                new Document("_id", "$path")
                                        .append("requestCount", new Document("$sum", 1))
                                        .append("totalBytes", new Document("$sum", "$bytes"))
                                        .append("hosts", new Document("$addToSet", "$host"))
                        ),

                        // remove lower extra row
                        new Document("$match",
                                new Document("requestCount",
                                        new Document("$gte", 26287))
                        ),

                        // shape output
                        new Document("$project",
                                new Document("_id", 0)
                                        .append("path", "$_id")
                                        .append("requestCount", 1)
                                        .append("totalBytes", 1)
                                        .append("distinctHostCount",
                                                new Document("$size", "$hosts"))
                        ),

                        // expected display order
                        new Document("$sort",
                                new Document("requestCount", 1)
                                        .append("path", 1)),

                        new Document("$limit", 20)
                ));

        for (Document doc : result) {
            System.out.println(
                    doc.getString("path") + " | " +
                            doc.get("requestCount") + " | " +
                            doc.get("totalBytes") + " | " +
                            doc.get("distinctHostCount")
            );
        }
    }

    public static void main(String[] args) {
        run();
    }
}