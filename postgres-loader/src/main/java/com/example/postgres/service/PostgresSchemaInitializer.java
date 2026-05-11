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
    public  static int initializeGlobal(){
        // global db only called by main /// not FLUSH
        return 0;
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