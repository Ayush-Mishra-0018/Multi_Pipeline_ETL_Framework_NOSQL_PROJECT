package com.example.service;

import com.example.config.AppProperties;

import java.sql.*;
import java.util.List;
import java.util.Map;

public class PostgresInsertService {

    private static final String URL =
            AppProperties.get("postgres.url");

    private static final String USER =
            AppProperties.get("postgres.username");

    private static final String PASSWORD =
            AppProperties.get("postgres.password");


    public static void flushAndInsert(
            String tableName,
            List<Map<String, Object>> rows
    ) {

        try (
                Connection conn =
                        DriverManager.getConnection(URL, USER, PASSWORD)
        ) {

            conn.setAutoCommit(false);

            // flush previous data
            Statement st = conn.createStatement();
            st.executeUpdate("TRUNCATE TABLE " + tableName);

            if (rows == null || rows.isEmpty()) {
                conn.commit();
                System.out.println("Old data flushed. No new rows.");
                return;
            }

            Map<String, Object> firstRow = rows.get(0);

            StringBuilder cols = new StringBuilder();
            StringBuilder vals = new StringBuilder();

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
                    "INSERT INTO " + tableName +
                            " (" + cols + ") VALUES (" + vals + ")";

            PreparedStatement ps =
                    conn.prepareStatement(sql);

            for (Map<String, Object> row : rows) {

                int index = 1;

                for (Object value : row.values()) {
                    ps.setObject(index++, value);
                }

                ps.addBatch();
            }

            ps.executeBatch();
            conn.commit();

            System.out.println(
                    "Flushed + Inserted " +
                            rows.size() +
                            " rows into " +
                            tableName
            );

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}