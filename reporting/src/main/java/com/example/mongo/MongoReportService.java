package com.example.mongo;

import com.example.mongo.service.MongoConnection;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import static com.mongodb.client.model.Sorts.descending;

public final class MongoReportService {

    private MongoReportService() {
    }

    public static Document getLatestPipelineMetadata() {

        MongoDatabase database =
                MongoConnection.getDatabase();

        MongoCollection<Document> metaCollection =
                database.getCollection(
                        "pipeline_run_metadata"
                );

        return metaCollection
                .find()
                .sort(descending("_id"))
                .first();
    }
}