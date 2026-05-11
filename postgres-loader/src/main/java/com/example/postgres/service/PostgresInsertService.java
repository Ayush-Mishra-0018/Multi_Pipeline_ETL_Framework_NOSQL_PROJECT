package com.example.postgres.service;

import com.example.config.ConfigReader;

import org.bson.Document;

import java.sql.*;
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
            String pipelineName,
            List<Integer> queries,
            long totalRuntime,
            Document meta
    ) {

        int runId = -1;

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
                            "pipeline_name, " +
                            "query_name, " +
                            "runtime" +
                            ") " +
                            "VALUES (?, ?, ?)";

            try (
                    PreparedStatement ps =
                            conn.prepareStatement(
                                    runSql,
                                    Statement.RETURN_GENERATED_KEYS
                            );
            ) {

                for (Integer query : queries) {

                    ps.setString(
                            1,
                            pipelineName
                    );

                    ps.setInt(
                            2,
                            query
                    );

                    ps.setDouble(
                            3,
                            totalRuntime
                    );

                    ps.executeUpdate();

                    ResultSet rs =
                            ps.getGeneratedKeys();



                    if (rs.next()) {

                        runId = rs.getInt(1);
                    }
                }
            }

            // =========================================
            // EXTRACT MONGO METADATA
            // =========================================

            int totalRecords =
                    ((Number) meta.getOrDefault("totalRecords", 0)).intValue();

            int totalValid =
                    ((Number) meta.getOrDefault("totalValid", 0)).intValue();

            int totalMalformed =
                    ((Number) meta.getOrDefault("totalMalformed", 0)).intValue();

            int totalBatches =
                    ((Number) meta.getOrDefault("totalBatches", 0)).intValue();

            double avgBatchSize =
                    ((Number) meta.getOrDefault("avgBatchSize", 0)).doubleValue();

            int executionTimeMs =
                    ((Number) meta.getOrDefault("executionTimeMs", 0)).intValue();



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


            conn.commit();
            System.out.println(
                    "Global metadata inserted successfully."
            );

        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}