package com.example.mongo.queries;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.AggregateIterable;

import org.bson.Document;

import java.util.Arrays;

public class Query3_HourlyErrorAnalysis {

    public static void run(MongoDatabase database) {

        MongoCollection<Document> collection =
                database.getCollection("filtered_logs");

        AggregateIterable<Document> result = collection.aggregate(Arrays.asList(

                // STEP 1: Group by date + hour
                new Document("$group",
                        new Document("_id",
                                new Document("log_date", "$log_date")
                                        .append("log_hour", "$log_hour")
                        )

                                // Count error requests (400–599)
                                .append("error_request_count",
                                        new Document("$sum",
                                                new Document("$cond", Arrays.asList(
                                                        new Document("$and", Arrays.asList(
                                                                new Document("$gte", Arrays.asList("$status_code", 400)),
                                                                new Document("$lte", Arrays.asList("$status_code", 599))
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
                                                                new Document("$gte", Arrays.asList("$status_code", 400)),
                                                                new Document("$lte", Arrays.asList("$status_code", 599))
                                                        )),
                                                        "$host",
                                                        "$$REMOVE"
                                                ))
                                        )
                                )
                ),

                // STEP 2: Calculate error rate
                new Document("$project",
                        new Document("_id", 0)
                                .append("log_date", "$_id.log_date")
                                .append("log_hour", "$_id.log_hour")
                                .append("error_request_count", 1)
                                .append("total_request_count", 1)

                                .append("error_rate",
                                        new Document("$cond", Arrays.asList(
                                                new Document("$eq", Arrays.asList("$total_request_count", 0)),
                                                0,
                                                new Document("$divide", Arrays.asList(
                                                        "$error_request_count",
                                                        "$total_request_count"
                                                ))
                                        ))
                                )

                                .append("distinct_error_hosts",
                                        new Document("$size", "$distinct_error_hosts")
                                )
                ),

                // STEP 3: Sort by date + hour
                new Document("$sort",
                        new Document("log_date", 1)
                                .append("log_hour", 1)
                )
        ));

        // PRINT OUTPUT
        for (Document doc : result) {
            System.out.println(doc.toJson());
        }
    }
}