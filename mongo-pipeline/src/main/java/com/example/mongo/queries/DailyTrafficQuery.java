package com.example.mongo.queries;

import com.example.mongo.service.MongoConnection;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;

/**
 * Query 1 – Daily Traffic Summary
 *
 * For each (log_date, status_code) pair, computes:
 *   - total number of requests
 *   - total bytes transferred
 *
 * Source collection : filtered_logs  (pre-cleaned; no malformed records)
 * Output columns    : log_date | status_code | request_count | total_bytes
 *
 * Results are sorted by log_date ASC, then status_code ASC.
 */
public final class DailyTrafficQuery {

    // ── collection names ────────────────────────────────────────────────────
    private static final String FILTERED_LOGS   = "filtered_logs";
    private static final String QUERY1_RESULTS  = "query1_daily_traffic";

    // ── field names in filtered_logs ─────────────────────────────────────────
    private static final String F_DATE          = "date";
    private static final String F_STATUS        = "status";
    private static final String F_BYTES         = "bytes";

    // ── field names in query1_daily_traffic ─────────────────────────────────
    private static final String R_LOG_DATE      = "log_date";
    private static final String R_STATUS_CODE   = "status_code";
    private static final String R_REQUEST_COUNT = "request_count";
    private static final String R_TOTAL_BYTES   = "total_bytes";

    private DailyTrafficQuery() {}

    // ────────────────────────────────────────────────────────────────────────
    // Public API
    // ────────────────────────────────────────────────────────────────────────

    /**
     * Runs the aggregation pipeline against {@code parsed_logs},
     * persists results to {@code query1_daily_traffic}, and returns
     * the result documents so the caller can forward them to Postgres.
     *
     * @param pipelineName  label stored alongside results (e.g. "mongodb")
     * @param runId         unique run identifier for this execution
     * @return              list of result documents (one per date+status pair)
     */
    public static List<Document> run(String pipelineName, String runId) {

        MongoDatabase db = MongoConnection.getDatabase();

        MongoCollection<Document> source =
                db.getCollection(FILTERED_LOGS);

        MongoCollection<Document> sink =
                db.getCollection(QUERY1_RESULTS);

        // ── build the aggregation pipeline ──────────────────────────────────
        //
        //  Stage 1 – $group  : group by (date, status); sum requests & bytes
        //  Stage 2 – $sort   : log_date ASC, status_code ASC
        //  Stage 3 – $project: reshape to final output schema
        //
        //  No $match needed – filtered_logs contains only clean records.

        Document groupStage = new Document("$group",
                new Document("_id",
                        new Document(R_LOG_DATE,    "$" + F_DATE)
                                .append(R_STATUS_CODE, "$" + F_STATUS)
                )
                .append(R_REQUEST_COUNT, new Document("$sum", 1))
                .append(R_TOTAL_BYTES,   new Document("$sum", "$" + F_BYTES))
        );

        Document sortStage = new Document("$sort",
                new Document("_id." + R_LOG_DATE,   1)
                        .append("_id." + R_STATUS_CODE, 1)
        );

        Document projectStage = new Document("$project",
                new Document("_id", 0)
                        .append(R_LOG_DATE,      "$_id." + R_LOG_DATE)
                        .append(R_STATUS_CODE,   "$_id." + R_STATUS_CODE)
                        .append(R_REQUEST_COUNT, 1)
                        .append(R_TOTAL_BYTES,   1)
        );

        List<Document> pipeline = Arrays.asList(
                groupStage,
                sortStage,
                projectStage
        );

        // ── execute & collect ────────────────────────────────────────────────
        List<Document> results = new java.util.ArrayList<>();

        try (MongoCursor<Document> cursor =
                     source.aggregate(pipeline).cursor()) {

            while (cursor.hasNext()) {
                Document row = cursor.next();

                // Stamp metadata so each result row is self-describing
                row.append("pipeline", pipelineName)
                   .append("runId",    runId)
                   .append("query",    "Q1_DailyTrafficSummary")
                   .append("savedAt",  java.time.Instant.now().toString());

                results.add(row);
            }
        }

        // ── persist to Mongo result collection (drop-then-insert pattern) ───
        sink.drop();
        if (!results.isEmpty()) {
            sink.insertMany(results);
        }

        System.out.printf(
                "[Q1] Daily Traffic Summary complete – %d (date, status) pairs written%n",
                results.size()
        );

        return results;
    }

    // ────────────────────────────────────────────────────────────────────────
    // Pretty-print helper (useful for quick CLI verification)
    // ────────────────────────────────────────────────────────────────────────

    public static void printResults(List<Document> results) {

        System.out.printf(
                "%n%-12s  %-7s  %15s  %15s%n",
                "log_date", "status", "request_count", "total_bytes"
        );
        System.out.println("-".repeat(55));

        for (Document row : results) {
            System.out.printf(
                    "%-12s  %-7d  %15d  %15d%n",
                    row.getString(R_LOG_DATE),
                    row.getInteger(R_STATUS_CODE),
                    row.getInteger(R_REQUEST_COUNT),
                    row.getLong(R_TOTAL_BYTES)
            );
        }
    }
}
