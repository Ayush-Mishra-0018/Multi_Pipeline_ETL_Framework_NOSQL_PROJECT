package com.example.postgres.service;

import com.example.config.ConfigReader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public final class PostgresSchemaInitializer {

    private static final String BASE_URL =
            ConfigReader.get("postgres.url");

    private static final String USER =
            ConfigReader.get("postgres.username");

    private static final String PASSWORD =
            ConfigReader.get("postgres.password");

    private PostgresSchemaInitializer() {
    }

    // called by main and never flushed
    public static void initializeGlobal() {

        String globalDatabase = "global_db";

        try {

            createDatabaseIfMissing(globalDatabase);

            String jdbcUrl =
                    BASE_URL + "/" + globalDatabase;

            try (
                    Connection conn =
                            DriverManager.getConnection(
                                    jdbcUrl,
                                    USER,
                                    PASSWORD
                            );

                    Statement st =
                            conn.createStatement()
            ) {

                st.executeUpdate(
                        "CREATE TABLE IF NOT EXISTS run_metadata (" +
                                "run_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY," +
                                "pipeline_name VARCHAR(255) NOT NULL," +

                                "query_name INTEGER NOT NULL " +
                                "CHECK (query_name >= 1 AND query_name <= 4)," +

                                "runtime NUMERIC(10,3) NOT NULL," +

                                "execution_timestamp TIMESTAMP WITH TIME ZONE " +
                                "NOT NULL DEFAULT CURRENT_TIMESTAMP" +
                                ")"
                );

                // Create batch_metadata table
                st.executeUpdate(
                        "CREATE TABLE IF NOT EXISTS batch_metadata (" +
                                "run_id INTEGER PRIMARY KEY " +
                                "REFERENCES run_metadata(run_id)," +

                                "pipeline_name VARCHAR(255) NOT NULL," +

                                "total_records INTEGER NOT NULL," +
                                "total_valid INTEGER NOT NULL," +

                                "total_malformed INTEGER NOT NULL DEFAULT 0," +

                                "total_batches INTEGER NOT NULL," +

                                "avg_batch_size DOUBLE PRECISION NOT NULL," +

                                "execution_time_ms INTEGER NOT NULL," +

                                "timestamp TIMESTAMP WITH TIME ZONE " +
                                "NOT NULL DEFAULT CURRENT_TIMESTAMP" +
                                ")"
                );

            }

        } catch (Exception e) {

            e.printStackTrace();

        }
    }
    public static void initialize( // this always flushes
            String databaseName // mongo hive pig map
    ) {

        try {

            createDatabaseIfMissing(databaseName);

            createTablesIfMissing(databaseName);

            System.out.println(
                    "PostgreSQL schema initialized successfully."
            );

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createDatabaseIfMissing(
            String databaseName
    ) throws Exception {

        try (
                Connection conn =
                        DriverManager.getConnection(
                                BASE_URL + "/postgres",
                                USER,
                                PASSWORD
                        );

                Statement st =
                        conn.createStatement()
        ) {

            ResultSet rs =
                    st.executeQuery(
                            "SELECT 1 FROM pg_database " +
                                    "WHERE datname = '" +
                                    databaseName + "'"
                    );

            if (!rs.next()) {

                st.executeUpdate(
                        "CREATE DATABASE " + databaseName
                );

                System.out.println(
                        "Database created: " +
                                databaseName
                );

            } else {

                System.out.println(
                        "Database already exists: " +
                                databaseName
                );
            }
        }
    }

    private static void truncateTables(
            Statement st
    ) throws Exception {

        try {

            st.executeQuery(
                    "SELECT * FROM query_1 LIMIT 1"
            );

            st.executeQuery(
                    "SELECT * FROM query_2 LIMIT 1"
            );

            st.executeQuery(
                    "SELECT * FROM query_3 LIMIT 1"
            );

            st.executeUpdate(
                    "TRUNCATE TABLE " +
                            "query_1, " +
                            "query_2, " +
                            "query_3 " +
                            "RESTART IDENTITY"
            );

            System.out.println(
                    "Existing tables truncated."
            );

        } catch (Exception e) {

            System.out.println(
                    "Tables do not exist yet. Skipping truncate."
            );
        }
    }

    private static void createTablesIfMissing(
            String databaseName
    ) throws Exception {

        String jdbcUrl =
                BASE_URL + "/" + databaseName;

        try (
                Connection conn =
                        DriverManager.getConnection(
                                jdbcUrl,
                                USER,
                                PASSWORD
                        );

                Statement st =
                        conn.createStatement()
        ) {


            truncateTables(st);

            st.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS query_1 (" +
                            "id SERIAL PRIMARY KEY," +
                            "log_date DATE," +
                            "status_code INT," +
                            "request_count BIGINT," +
                            "total_bytes BIGINT," +
                            "batch_id TEXT," +
                            "run_id TEXT," +
                            "pipeline_name TEXT," +
                            "executed_at TIMESTAMP" +
                            ")"
            );

            st.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS query_2 (" +
                            "id SERIAL PRIMARY KEY," +
                            "resource_path TEXT," +
                            "request_count INT," +
                            "total_bytes BIGINT," +
                            "distinct_hosts INT," +
                            "batch_id TEXT," +
                            "run_id TEXT," +
                            "pipeline_name TEXT," +
                            "executed_at TIMESTAMP" +
                            ")"
            );

            st.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS query_3 (" +
                            "id SERIAL PRIMARY KEY," +
                            "log_date TEXT," +

                            "log_hour INT," +
                            "error_request_count INT," +
                            "total_request_count INT," +
                            "error_rate DOUBLE PRECISION," +
                            "distinct_error_hosts INT," +
                            "batch_id TEXT," +
                            "run_id TEXT," +
                            "pipeline_name TEXT," +
                            "executed_at TIMESTAMP" +
                            ")"
            );
        }
    }
}