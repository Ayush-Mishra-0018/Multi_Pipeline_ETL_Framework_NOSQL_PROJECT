package com.example.postgres.service;

import com.example.config.ConfigReader;

import org.bson.Document;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public final class PostgresInsertService {

    private static final String BASE_URL =
            ConfigReader.get("postgres.url");

    private static final String USER =
            ConfigReader.get("postgres.username");

    private static final String PASSWORD =
            ConfigReader.get("postgres.password");

    private PostgresInsertService() {
    }

    public static void Insert(
            String databaseName,
            String tableName,
            List<Map<String, Object>> rows
    ) {

        String jdbcUrl =
                BASE_URL + "/" + databaseName;

        try (
                Connection conn =
                        DriverManager.getConnection(
                                jdbcUrl,
                                USER,
                                PASSWORD
                        )
        ) {

            conn.setAutoCommit(false);

//            Statement st =
//                    conn.createStatement();
//
//            st.executeUpdate(
//                    "TRUNCATE TABLE " + tableName // now this logic will be handled by init file
//            );

            if (rows == null || rows.isEmpty()) {

                conn.commit();

                System.out.println(
                        "No rows found for table: " +
                                tableName
                );

                return;
            }

            Map<String, Object> firstRow =
                    rows.get(0);

            StringBuilder cols =
                    new StringBuilder();

            StringBuilder vals =
                    new StringBuilder();

            int i = 0;

            for (String col : firstRow.keySet()) {

                cols.append(col);

                vals.append("?");

                if (i < firstRow.size() - 1) {

                    cols.append(",");

                    vals.append(",");
                }

                i++;
            }

            String sql =
                    "INSERT INTO " +
                            tableName +
                            " (" +
                            cols +
                            ") VALUES (" +
                            vals +
                            ")";

            PreparedStatement ps =
                    conn.prepareStatement(sql);

            for (Map<String, Object> row : rows) {

                int index = 1;

                for (Object value : row.values()) {

                    ps.setObject(
                            index++,
                            value
                    );
                }

                ps.addBatch();
            }

            ps.executeBatch();

            conn.commit();

            System.out.println(
                    "Inserted " +
                            rows.size() +
                            " rows into " +
                            tableName +
                            " in database " +
                            databaseName
            );

        } catch (Exception e) {

            e.printStackTrace();
        }
    }



    public static void insertGlobalMetadata(
            int runId,
            String pipelineName,
            List<Integer> queries,
            long totalRuntime,
            Document meta
    ) {

        String globalDatabase = "global_db";

        String jdbcUrl =
                BASE_URL + "/" + globalDatabase;

        try (
                Connection conn =
                        DriverManager.getConnection(
                                jdbcUrl,
                                USER,
                                PASSWORD
                        )
        ) {

            // =========================================
            // INSERT INTO run_metadata
            // =========================================

            String runSql =
                    "INSERT INTO run_metadata (" +
                            "run_id, " +
                            "pipeline_name, " +
                            "query_name, " +
                            "runtime" +
                            ") " +
                            "VALUES (?, ?, ?, ?)";

            try (
                    PreparedStatement ps =
                            conn.prepareStatement(runSql)
            ) {

                for (Integer query : queries) {

                    ps.setInt(1, runId);

                    ps.setString(
                            2,
                            pipelineName
                    );

                    ps.setInt(
                            3,
                            query
                    );

                    ps.setDouble(
                            4,
                            totalRuntime
                    );

                    ps.executeUpdate();
                }
            }

            // =========================================
            // EXTRACT MONGO METADATA
            // =========================================

            int totalRecords =
                    meta.getInteger("totalRecords", 0);

            int totalValid =
                    meta.getInteger("totalValid", 0);

            int totalMalformed =
                    meta.getInteger("totalMalformed", 0);

            int totalBatches =
                    meta.getInteger("totalBatches", 0);

            double avgBatchSize =
                    meta.getDouble("avgBatchSize");

            int executionTimeMs =
                    meta.getInteger(
                            "executionTimeMs",
                            0
                    );

            // =========================================
            // INSERT INTO batch_metadata
            // =========================================

            String batchSql =
                    "INSERT INTO batch_metadata (" +
                            "run_id, " +
                            "pipeline_name, " +
                            "total_records, " +
                            "total_valid, " +
                            "total_malformed, " +
                            "total_batches, " +
                            "avg_batch_size, " +
                            "execution_time_ms" +
                            ") " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

            try (
                    PreparedStatement ps =
                            conn.prepareStatement(batchSql)
            ) {

                ps.setInt(1, runId);

                ps.setString(
                        2,
                        pipelineName
                );

                ps.setInt(
                        3,
                        totalRecords
                );

                ps.setInt(
                        4,
                        totalValid
                );

                ps.setInt(
                        5,
                        totalMalformed
                );

                ps.setInt(
                        6,
                        totalBatches
                );

                ps.setDouble(
                        7,
                        avgBatchSize
                );

                ps.setInt(
                        8,
                        executionTimeMs
                );

                ps.executeUpdate();
            }

            System.out.println(
                    "Global metadata inserted successfully."
            );

        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}