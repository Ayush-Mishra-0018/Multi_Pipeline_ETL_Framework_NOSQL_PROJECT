package com.example.postgres.service;

import com.example.config.ConfigReader;

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
}