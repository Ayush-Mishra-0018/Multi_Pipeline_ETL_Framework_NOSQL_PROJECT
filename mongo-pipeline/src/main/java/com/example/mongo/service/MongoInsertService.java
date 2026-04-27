package com.example.mongo.service;

import com.example.model.BatchResult;
import com.example.model.ParsedLog;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public final class MongoInsertService {

    private static final String BATCH_METADATA = "batch_metadata";
    private static final String PARSED_LOGS = "parsed_logs";

    private static final MongoDatabase DATABASE =
            MongoConnection.getDatabase();

    private static final MongoCollection<Document> METADATA_COLLECTION =
            DATABASE.getCollection(BATCH_METADATA);

    private static final MongoCollection<Document> LOG_COLLECTION =
            DATABASE.getCollection(PARSED_LOGS);

    private MongoInsertService() {
    }

    public static void insertBatchMetadata(BatchResult result) {

        Document doc = new Document()
                .append("batchId", result.getBatchId())
                .append("totalRecords", result.getTotalRecords())
                .append("malformedRecords", result.getMalformedRecords())
                .append("validRecords", result.getValidRecords())
                .append("insertedAt", Instant.now().toString());

        METADATA_COLLECTION.insertOne(doc);
    }

    public static void insertParsedLogs(BatchResult result) {

        List<ParsedLog> logs = result.getParsedLogs();

        List<Document> docs = new ArrayList<>(logs.size());

        for (ParsedLog log : logs) {
            docs.add(toDocument(log));
        }

        if (!docs.isEmpty()) {
            LOG_COLLECTION.insertMany(docs);
        }
    }

    private static Document toDocument(ParsedLog log) {

        return new Document()
                .append("host", log.getHost())
                .append("rawTimestamp", log.getRawTimestamp())
                .append("date", log.getDate())
                .append("hour", log.getHour())
                .append("method", log.getMethod())
                .append("path", log.getPath())
                .append("protocol", log.getProtocol())
                .append("status", log.getStatus())
                .append("bytes", log.getBytes())
                .append("malformed", log.isMalformed())
                .append("batchId", log.getBatchId());
    }
}