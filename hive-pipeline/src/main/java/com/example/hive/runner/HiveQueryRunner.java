package com.example.hive.runner;

import com.example.hive.service.HiveProcessRunner;
import com.example.hive.service.HiveScriptBuilder;
import com.example.postgres.service.PostgresInsertService;
import com.example.postgres.service.PostgresSchemaInitializer;

import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * HiveQueryRunner
 *
 * Mirrors MongoQueryRunner in structure.
 *
 * Java responsibilities (ONLY):
 *   1. Select the HQL script for each query
 *   2. Invoke Hive via HiveProcessRunner (ProcessBuilder)
 *   3. Read TSV output lines from Hive stdout
 *   4. Build List<Map<String,Object>> row lists
 *   5. Insert final aggregated rows into PostgreSQL
 *   6. Print result table to console
 *   7. Measure runtime
 *
 * Java does NOT:
 *   - Aggregate, filter, or transform any data
 *   - Parse log lines
 *   - Perform joins, groupings, or counts
 *   All of that is done entirely in the .hql scripts by HiveQL.
 */
public final class HiveQueryRunner {

    private static final DateTimeFormatter NASA_DATE_FMT =
            DateTimeFormatter.ofPattern("dd/MMM/yyyy", Locale.ENGLISH);

    private static final DateTimeFormatter ISO_DATE_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private HiveQueryRunner() {
    }

    // =================================================================
    // PUBLIC ENTRY POINT
    // =================================================================

    public static long runQueries(
            List<Integer> queries
    ) {

        long startTime = System.currentTimeMillis();

        // Initialize PostgreSQL schema for the hive database
        PostgresSchemaInitializer.initialize("hive");

        for (int query : queries) {

            switch (query) {

                case 1:
                    runQuery1();
                    break;

                case 2:
                    runQuery2();
                    break;

                case 3:
                    runQuery3();
                    break;

                default:
                    System.out.println(
                            "[HiveQueryRunner] Unknown query: " + query
                    );
            }
        }

        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }

    // =================================================================
    // QUERY 1 — Daily Traffic Summary
    // Output columns from HQL:
    //   log_date (dd/MMM/yyyy) | status_code | request_count |
    //   total_bytes | batch_id
    // =================================================================

    private static void runQuery1() {

        System.out.println(
                "\n[HiveQueryRunner] Running Query 1: Daily Traffic Summary..."
        );

        try {

            String scriptPath =
                    HiveScriptBuilder.getScriptPath("hive_query_1.hql");

            List<String> outputLines =
                    HiveProcessRunner.runScript(
                            scriptPath,
                            HiveScriptBuilder.hdfsBaseConf()
                    );

            List<Map<String, Object>> rows = new ArrayList<>();

            for (String line : outputLines) {

                if (line.isBlank()) continue;

                // TSV: log_date \t status_code \t request_count \t total_bytes \t batch_id
                String[] parts = line.split("\t");

                if (parts.length < 5) continue;

                try {

                    // Convert '01/Jul/1995' → java.sql.Date
                    String rawDate = parts[0].trim();
                    java.sql.Date sqlDate = parseDateToSql(rawDate);

                    int statusCode     = Integer.parseInt(parts[1].trim());
                    long requestCount  = Long.parseLong(parts[2].trim());
                    long totalBytes    = Long.parseLong(parts[3].trim());
                    String batchId     = parts[4].trim();

                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("log_date",      sqlDate);
                    row.put("status_code",   statusCode);
                    row.put("request_count", requestCount);
                    row.put("total_bytes",   totalBytes);
                    row.put("batch_id",      sortBatchIdString(batchId));

                    rows.add(row);

                } catch (Exception e) {
                    System.err.println(
                            "[HiveQueryRunner] Skipping malformed output line: " + line
                    );
                }
            }

            // Insert final aggregated results into PostgreSQL
            PostgresInsertService.Insert("hive", "query_1", rows);

            // Print to console
            printQuery1(rows);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // =================================================================
    // QUERY 2 — Top 20 Requested Resources
    // Output columns from HQL:
    //   resource_path | request_count | total_bytes |
    //   distinct_hosts | batch_id
    // =================================================================

    private static void runQuery2() {

        System.out.println(
                "\n[HiveQueryRunner] Running Query 2: Top Requested Resources..."
        );

        try {

            String scriptPath =
                    HiveScriptBuilder.getScriptPath("hive_query_2.hql");

            List<String> outputLines =
                    HiveProcessRunner.runScript(
                            scriptPath,
                            HiveScriptBuilder.hdfsBaseConf()
                    );

            List<Map<String, Object>> rows = new ArrayList<>();

            for (String line : outputLines) {

                if (line.isBlank()) continue;

                // TSV: resource_path \t request_count \t total_bytes \t distinct_hosts \t batch_id
                String[] parts = line.split("\t");

                if (parts.length < 5) continue;

                try {

                    String resourcePath   = parts[0].trim();
                    long   requestCount   = Long.parseLong(parts[1].trim());
                    long   totalBytes     = Long.parseLong(parts[2].trim());
                    int    distinctHosts  = Integer.parseInt(parts[3].trim());
                    String batchId        = parts[4].trim();

                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("resource_path",  resourcePath);
                    row.put("request_count",  (int) requestCount);
                    row.put("total_bytes",    totalBytes);
                    row.put("distinct_hosts", distinctHosts);
                    row.put("batch_id",       sortBatchIdString(batchId));

                    rows.add(row);

                } catch (Exception e) {
                    System.err.println(
                            "[HiveQueryRunner] Skipping malformed output line: " + line
                    );
                }
            }

            // Insert final aggregated results into PostgreSQL
            PostgresInsertService.Insert("hive", "query_2", rows);

            // Print to console
            printQuery2(rows);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // =================================================================
    // QUERY 3 — Hourly Error Analysis
    // Output columns from HQL:
    //   log_date | log_hour | error_request_count | total_request_count |
    //   error_rate | distinct_error_hosts | batch_id
    // =================================================================

    private static void runQuery3() {

        System.out.println(
                "\n[HiveQueryRunner] Running Query 3: Hourly Error Analysis..."
        );

        try {

            String scriptPath =
                    HiveScriptBuilder.getScriptPath("hive_query_3.hql");

            List<String> outputLines =
                    HiveProcessRunner.runScript(
                            scriptPath,
                            HiveScriptBuilder.hdfsBaseConf()
                    );

            List<Map<String, Object>> rows = new ArrayList<>();

            for (String line : outputLines) {

                if (line.isBlank()) continue;

                // TSV: log_date \t log_hour \t error_count \t total_count \t error_rate \t distinct_hosts \t batch_id
                String[] parts = line.split("\t");

                if (parts.length < 7) continue;

                try {

                    String logDate          = parts[0].trim();
                    int    logHour          = Integer.parseInt(parts[1].trim());
                    int    errorCount       = Integer.parseInt(parts[2].trim());
                    int    totalCount       = Integer.parseInt(parts[3].trim());
                    double errorRate        = Double.parseDouble(parts[4].trim());
                    int    distinctErrHosts = Integer.parseInt(parts[5].trim());
                    String batchId          = parts[6].trim();

                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("log_date",              logDate);
                    row.put("log_hour",               logHour);
                    row.put("error_request_count",    errorCount);
                    row.put("total_request_count",    totalCount);
                    row.put("error_rate",             errorRate);
                    row.put("distinct_error_hosts",   distinctErrHosts);
                    row.put("batch_id",               sortBatchIdString(batchId));

                    rows.add(row);

                } catch (Exception e) {
                    System.err.println(
                            "[HiveQueryRunner] Skipping malformed output line: " + line
                    );
                }
            }

            // Insert final aggregated results into PostgreSQL
            PostgresInsertService.Insert("hive", "query_3", rows);

            // Print to console
            printQuery3(rows);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // =================================================================
    // PRINT HELPERS
    // =================================================================

    private static void printQuery1(
            List<Map<String, Object>> rows
    ) {
        System.out.printf(
                "%n%-12s | %-12s | %-15s | %-15s | %-30s%n",
                "log_date", "status_code", "request_count",
                "total_bytes", "batch_id"
        );
        System.out.println("-".repeat(95));
        for (Map<String, Object> row : rows) {
            System.out.printf(
                    "%-12s | %-12d | %-15d | %-15d | %-30s%n",
                    row.get("log_date"),
                    row.get("status_code"),
                    row.get("request_count"),
                    row.get("total_bytes"),
                    row.get("batch_id")
            );
        }
    }

    private static void printQuery2(
            List<Map<String, Object>> rows
    ) {
        System.out.printf(
                "%n%-50s | %-14s | %-14s | %-15s | %-20s%n",
                "resource_path", "request_count", "total_bytes",
                "distinct_hosts", "batch_id"
        );
        System.out.println("-".repeat(120));
        for (Map<String, Object> row : rows) {
            System.out.printf(
                    "%-50s | %-14d | %-14d | %-15d | %-20s%n",
                    row.get("resource_path"),
                    row.get("request_count"),
                    row.get("total_bytes"),
                    row.get("distinct_hosts"),
                    row.get("batch_id")
            );
        }
    }

    private static void printQuery3(
            List<Map<String, Object>> rows
    ) {
        System.out.printf(
                "%n%-12s | %-9s | %-20s | %-20s | %-10s | %-20s | %-20s%n",
                "log_date", "log_hour", "error_request_count",
                "total_request_count", "error_rate",
                "distinct_error_hosts", "batch_id"
        );
        System.out.println("-".repeat(120));
        for (Map<String, Object> row : rows) {
            System.out.printf(
                    "%-12s | %-9d | %-20d | %-20d | %-10.2f | %-20d | %-20s%n",
                    row.get("log_date"),
                    row.get("log_hour"),
                    row.get("error_request_count"),
                    row.get("total_request_count"),
                    row.get("error_rate"),
                    row.get("distinct_error_hosts"),
                    row.get("batch_id")
            );
        }
    }

    // =================================================================
    // DATE CONVERSION HELPER
    // Converts '01/Jul/1995' → java.sql.Date(1995-07-01)
    // Java only converts the date string format for Postgres storage.
    // =================================================================

    private static java.sql.Date parseDateToSql(String rawDate) {
        try {
            LocalDate localDate = LocalDate.parse(rawDate, NASA_DATE_FMT);
            return java.sql.Date.valueOf(localDate);
        } catch (Exception e) {
            // If already in ISO format (yyyy-MM-dd), parse directly
            try {
                return java.sql.Date.valueOf(rawDate);
            } catch (Exception e2) {
                throw new RuntimeException(
                        "Cannot parse date: " + rawDate, e2
                );
            }
        }
    }

    private static String sortBatchIdString(String rawBatchId) {
        if (rawBatchId == null || rawBatchId.isBlank()) return rawBatchId;
        try {
            List<Integer> batches = new ArrayList<>();
            for (String b : rawBatchId.split("\\+")) {
                batches.add(Integer.parseInt(b.trim()));
            }
            Collections.sort(batches);
            List<String> sortedStrings = new ArrayList<>();
            for (Integer b : batches) {
                sortedStrings.add(String.valueOf(b));
            }
            return String.join("+", sortedStrings);
        } catch (Exception e) {
            return rawBatchId; // fallback
        }
    }
}
