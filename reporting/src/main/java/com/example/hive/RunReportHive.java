package com.example.hive;

import com.example.config.ConfigReader;
import com.example.hive.dataSetup.HiveDataSetupExecutor;
import com.example.hive.runner.HiveQueryRunner;
import com.example.hive.service.HiveProcessRunner;
import com.example.hive.service.HiveScriptBuilder;
import com.example.model.MalformedRecord;
import com.example.model.PipelineExecutionResult;
import com.example.postgres.service.PostgresInsertService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * RunReportHive
 *
 * Top-level orchestrator for the Hive pipeline.
 * Mirrors RunReportMongo exactly in structure.
 *
 * Execution flow:
 *   Phase 1 – ETL Setup
 *       HiveDataSetupExecutor.execute()
 *         → uploads files to HDFS  (Java, file I/O only)
 *         → runs hive_setup.hql    (Hive: parse + clean + filter)
 *         → collects stats via Hive COUNT(*) queries
 *         → returns PipelineExecutionResult
 *
 *   Phase 2 – Queries
 *       HiveQueryRunner.runQueries(queries)
 *         → runs hive_query_N.hql  (Hive: group + aggregate)
 *         → reads TSV output from Hive stdout
 *         → inserts final rows into PostgreSQL
 *
 *   Phase 3 – Global metadata → global_db
 *   Phase 4 – Malformed records → hive.malformed_record_summary
 */
public class RunReportHive {

    public static void reporting(
            List<Integer> queries
    ) {

        try {

            System.out.println(
                    "\n=================================================="
            );
            System.out.println(
                    "           HIVE PIPELINE STARTING"
            );
            System.out.println(
                    "=================================================="
            );

            // =====================================================
            // PHASE 1: ETL setup (HDFS upload + hive_setup.hql)
            // =====================================================

            System.out.println(
                    "\n[RunReportHive] Phase 1: ETL setup..."
            );

            PipelineExecutionResult executionResult =
                    HiveDataSetupExecutor.execute();

            long pipelineRuntime =
                    executionResult.getExecutionTime();

            List<MalformedRecord> malformedRecords =
                    executionResult.getMalformedRecords();

            System.out.println(
                    "[RunReportHive] ETL done in " + pipelineRuntime +
                    " ms | malformed sampled: " + malformedRecords.size()
            );

            // =====================================================
            // PHASE 2: Run HiveQL queries → PostgreSQL
            // =====================================================

            System.out.println(
                    "\n[RunReportHive] Phase 2: Running queries " +
                    queries + "..."
            );

            long queryRuntime = HiveQueryRunner.runQueries(queries);

            System.out.println(
                    "[RunReportHive] Queries done in " +
                    queryRuntime + " ms."
            );

            // =====================================================
            // PHASE 3: Build metadata map and write to global_db
            // Counts come from Hive (already collected in Phase 1)
            // =====================================================

            long totalRuntime   = pipelineRuntime + queryRuntime;
            long totalMalformed = malformedRecords.size();
            int  totalBatches   = executionResult.getTotalBatches();

            // Ask Hive for the true total and valid record counts
            // (these are COUNT(*) queries — Hive computes, Java reads)
            long totalRecords = queryHiveCount("nasa_raw_logs");
            long totalValid   = queryHiveCount("nasa_filtered_logs");

            double avgBatchSize =
                    totalBatches == 0 ? 0
                            : (double) totalRecords / totalBatches;

            Map<String, Object> meta = new HashMap<>();
            meta.put("totalRecords",   (int) totalRecords);
            meta.put("totalValid",     (int) totalValid);
            meta.put("totalMalformed", (int) totalMalformed);
            meta.put("totalBatches",   totalBatches);
            meta.put("avgBatchSize",   avgBatchSize);
            meta.put("executionTimeMs",(int) pipelineRuntime);

            System.out.println(
                    "\n[RunReportHive] Phase 3: Writing global metadata..."
            );

            PostgresInsertService.insertGlobalMetadataMap(
                    "hive",
                    queries,
                    totalRuntime,
                    meta
            );

            // =====================================================
            // PHASE 4: Store malformed records in Postgres
            // =====================================================

            System.out.println(
                    "[RunReportHive] Phase 4: Writing malformed records..."
            );

            PostgresInsertService.insertMalformed(
                    "hive",
                    malformedRecords
            );

            // =====================================================
            // SUMMARY
            // =====================================================

            System.out.println(
                    "\n=================================================="
            );
            System.out.println("         HIVE PIPELINE COMPLETE");
            System.out.printf("  Pipeline  : hive%n");
            System.out.printf("  Queries   : %s%n",   queries);
            System.out.printf("  ETL time  : %d ms%n", pipelineRuntime);
            System.out.printf("  Query time: %d ms%n", queryRuntime);
            System.out.printf("  Total time: %d ms%n", totalRuntime);
            System.out.printf("  Batches   : %d%n",    totalBatches);
            System.out.printf("  Records   : %d%n",    totalRecords);
            System.out.printf("  Valid     : %d%n",    totalValid);
            System.out.printf("  Malformed : %d%n",    totalMalformed);
            System.out.printf("  Avg Batch : %.2f%n",  avgBatchSize);
            System.out.println(
                    "=================================================="
            );

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(
                    "[RunReportHive] Pipeline failed: " + e.getMessage()
            );
        }
    }

// =============================================================
    // Runs COUNT(*) on a Hive table; Hive computes, Java reads.
    // =============================================================

    private static long queryHiveCount(String tableName) {

        try {
            List<String> lines =
                    HiveProcessRunner.runInline(
                            "SELECT COUNT(*) FROM " + tableName + ";",
                            HiveScriptBuilder.hdfsBaseConf()
                    );

            for (String line : lines) {
                if (line.isBlank()) continue;

                String trimmed = line.trim();

                // Regex guard: Skip any log4j/INFO lines. Only parse pure digits.
                if (!trimmed.matches("\\d+")) continue;

                try {
                    return Long.parseLong(trimmed);
                } catch (NumberFormatException ignored) {
                }
            }
        } catch (Exception e) {
            System.err.println(
                    "[RunReportHive] Could not count " + tableName +
                            ": " + e.getMessage()
            );
        }
        return 0L;
    }
}
