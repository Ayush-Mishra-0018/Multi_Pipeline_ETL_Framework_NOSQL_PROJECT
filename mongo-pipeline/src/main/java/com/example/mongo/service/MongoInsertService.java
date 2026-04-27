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
    private static final String FILTERED_LOGS = "filtered_logs";
    private static final String PIPELINE_METADATA = "pipeline_run_metadata"; // ✅ NEW

    private static final MongoDatabase DATABASE =
            MongoConnection.getDatabase();

    private static final MongoCollection<Document> METADATA_COLLECTION =
            DATABASE.getCollection(BATCH_METADATA);

    private static final MongoCollection<Document> LOG_COLLECTION =
            DATABASE.getCollection(PARSED_LOGS);

    private static final MongoCollection<Document> FILTERED_LOG_COLLECTION =
            DATABASE.getCollection(FILTERED_LOGS);

    private static final MongoCollection<Document> PIPELINE_COLLECTION =
            DATABASE.getCollection(PIPELINE_METADATA); // ✅ NEW

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

    public static void clearCollections() {

        DATABASE.getCollection(PARSED_LOGS).deleteMany(new Document());
        DATABASE.getCollection(FILTERED_LOGS).deleteMany(new Document());
        DATABASE.getCollection(BATCH_METADATA).deleteMany(new Document());
        DATABASE.getCollection(PIPELINE_METADATA).deleteMany(new Document()); // ✅ NEW

        System.out.println("Mongo collections cleared.");
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

    public static void insertFilteredLogs(BatchResult result) {

        List<ParsedLog> logs = result.getParsedLogs();

        List<Document> docs = new ArrayList<>();

        for (ParsedLog log : logs) {

            if (!log.isMalformed()) {
                docs.add(toDocument(log));
            }
        }

        if (!docs.isEmpty()) {
            FILTERED_LOG_COLLECTION.insertMany(docs);
        }
    }

    // ✅ NEW METHOD (RUN SUMMARY)
    public static void insertPipelineSummary(
            long totalRecords,
            long totalValid,
            long totalMalformed,
            int totalBatches,
            double avgBatchSize,
            long executionTimeMs
    ) {

        Document doc = new Document()
                .append("totalRecords", totalRecords)
                .append("totalValid", totalValid)
                .append("totalMalformed", totalMalformed)
                .append("totalBatches", totalBatches)
                .append("avgBatchSize", avgBatchSize)
                .append("executionTimeMs", executionTimeMs)
                .append("timestamp", Instant.now().toString());

        PIPELINE_COLLECTION.insertOne(doc);
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