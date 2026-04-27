package com.example.mongo.queries;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Arrays;

public class Query3_HourlyErrorAnalysis {

    public static void run(MongoDatabase database) {


        MongoCollection<Document> collection =
                database.getCollection("filtered_logs");

        AggregateIterable<Document> result =
                collection.aggregate(Arrays.asList(

                        // Group by date + hour
                        new Document("$group",
                                new Document("_id",
                                        new Document("date", "$date")
                                                .append("hour", "$hour")
                                )

                                        // Error requests (400–599)
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

                                        // Total requests
                                        .append("total_request_count",
                                                new Document("$sum", 1)
                                        )

                                        // Distinct hosts causing errors
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
                        ),

                        // Final Output
                        new Document("$project",
                                new Document("_id", 0)
                                        .append("logdate", "$_id.date")
                                        .append("loghour", "$_id.hour")
                                        .append("errorrequestcount", "$error_request_count")
                                        .append("totalrequestcount", "$total_request_count")

                                        .append("errorrate",
                                                new Document("$round", Arrays.asList(
                                                        new Document("$multiply", Arrays.asList(
                                                                new Document("$divide", Arrays.asList(
                                                                        "$error_request_count",
                                                                        "$total_request_count"
                                                                )),
                                                                100
                                                        )),
                                                        2
                                                ))
                                        )

                                        .append("distincterrorhosts",
                                                new Document("$size", "$distinct_error_hosts")
                                        )
                        ),

                        // Sort
                        new Document("$sort",
                                new Document("logdate", 1)
                                        .append("loghour", 1)
                        )
                ));

        for (Document doc : result) {
            System.out.println(doc.toJson());
        }
    }
}