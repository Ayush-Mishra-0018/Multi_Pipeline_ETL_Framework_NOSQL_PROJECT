package com.example.mongo.queries;

import com.example.mongo.service.MongoConnection;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Arrays;

public final class Query1_DailyTrafficQuery {

    private Query1_DailyTrafficQuery() {}

    public static void run(MongoDatabase database) {

        MongoCollection<Document> collection =
                database.getCollection("filtered_logs");

        System.out.println("\n========== QUERY 1 : Daily Traffic Summary ==========");

        AggregateIterable<Document> result =
                collection.aggregate(Arrays.asList(

                        // Group by date + status code
                        new Document("$group",
                                new Document("_id",
                                        new Document("log_date", "$date")
                                                .append("status_code", "$status")
                                )
                                        .append("request_count",
                                                new Document("$sum", 1))
                                        .append("total_bytes",
                                                new Document("$sum", "$bytes"))
                        ),

                        // Reshape to final output schema
                        new Document("$project",
                                new Document("_id", 0)
                                        .append("log_date",      "$_id.log_date")
                                        .append("status_code",   "$_id.status_code")
                                        .append("request_count", 1)
                                        .append("total_bytes",   1)
                        ),

                        // Sort by date ASC, status ASC
                        new Document("$sort",
                                new Document("log_date", 1)
                                        .append("status_code", 1))
                ));

        for (Document doc : result) {
            System.out.println(doc.toJson());
        }
    }

    public static void main(String[] args) {
        run(MongoConnection.getDatabase());
    }
}