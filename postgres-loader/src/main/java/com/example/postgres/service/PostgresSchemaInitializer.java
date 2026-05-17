package com.example.postgres.service;

import com.example.config.ConfigReader;

import java.io.InputStream;
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

                InputStream inputStream =
                        PostgresSchemaInitializer.class
                                .getClassLoader()
                                .getResourceAsStream(
                                        "sql/global_schema.sql"
                                );

                if (inputStream == null) {

                    throw new RuntimeException(
                            "global_schema.sql not found"
                    );
                }

                String sql =
                        new String(
                                inputStream.readAllBytes()
                        );

                String[] statements =
                        sql.split(";");

                for (String query : statements) {

                    query = query.trim();

                    if (!query.isEmpty()) {

                        st.executeUpdate(query);
                    }
                }
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

            st.executeQuery(
                    "SELECT * FROM malformed_record_summary LIMIT 1"
            );

            st.executeUpdate(
                    "TRUNCATE TABLE " +
                            "query_1, " +
                            "query_2, " +
                            "query_3, " +
                            "malformed_record_summary " +
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

            InputStream inputStream =
                    PostgresSchemaInitializer.class
                            .getClassLoader()
                            .getResourceAsStream(
                                    "sql/pipeline_schema.sql"
                            );

            if (inputStream == null) {

                throw new RuntimeException(
                        "pipeline_schema.sql not found"
                );
            }

            String sql =
                    new String(
                            inputStream.readAllBytes()
                    );

            String[] statements =
                    sql.split(";");

            for (String query : statements) {

                query = query.trim();

                if (!query.isEmpty()) {

                    st.executeUpdate(query);
                }
            }
        }
    }
}