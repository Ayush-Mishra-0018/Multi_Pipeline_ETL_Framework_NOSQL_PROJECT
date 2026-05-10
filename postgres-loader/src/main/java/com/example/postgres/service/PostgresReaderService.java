package com.example.postgres.service;

import com.example.config.ConfigReader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.List;

public final class PostgresReaderService {

    private static final String BASE_URL =
            ConfigReader.get("postgres.url");

    private static final String USER =
            ConfigReader.get("postgres.username");

    private static final String PASSWORD =
            ConfigReader.get("postgres.password");

    private PostgresReaderService() {
    }

    public static void readAllRowsPretty(
            String databaseName,
            List<String> tableNames
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

            for (String table : tableNames) {

                System.out.println();

                printLine(150);

                System.out.println(
                        "TABLE : " +
                                table.toUpperCase()
                );

                printLine(150);

                String sql =
                        "SELECT * FROM " + table;

                Statement stmt =
                        conn.createStatement();

                ResultSet rs =
                        stmt.executeQuery(sql);

                ResultSetMetaData meta =
                        rs.getMetaData();

                int cols =
                        meta.getColumnCount();

                int[] widths =
                        getColumnWidths(meta, cols);

                // HEADER
                for (int i = 1; i <= cols; i++) {

                    System.out.printf(
                            "| %-" + widths[i - 1] + "s ",
                            fit(
                                    meta.getColumnName(i),
                                    widths[i - 1]
                            )
                    );
                }

                System.out.println("|");

                printLine(totalWidth(widths));

                // DATA
                while (rs.next()) {

                    for (int i = 1; i <= cols; i++) {

                        Object val =
                                rs.getObject(i);

                        String text =
                                val == null
                                        ? "null"
                                        : val.toString();

                        System.out.printf(
                                "| %-" + widths[i - 1] + "s ",
                                fit(
                                        text,
                                        widths[i - 1]
                                )
                        );
                    }

                    System.out.println("|");
                }

                printLine(totalWidth(widths));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static int[] getColumnWidths(
            ResultSetMetaData meta,
            int cols
    ) throws Exception {

        int[] widths =
                new int[cols];

        for (int i = 1; i <= cols; i++) {

            String name =
                    meta.getColumnName(i)
                            .toLowerCase();

            if (name.contains("resource_path")) {

                widths[i - 1] = 40;

            } else if (name.contains("batch_id")) {

                widths[i - 1] = 35;

            } else if (name.contains("run_id")) {

                widths[i - 1] = 15;

            } else if (name.contains("pipeline")) {

                widths[i - 1] = 12;

            } else if (name.contains("executed_at")) {

                widths[i - 1] = 28;

            } else {

                widths[i - 1] = 18;
            }
        }

        return widths;
    }

    private static String fit(
            String text,
            int width
    ) {

        if (text == null) {
            return "null";
        }

        if (text.length() <= width) {

            return text;
        }

        return text.substring(
                0,
                width - 3
        ) + "...";
    }

    private static int totalWidth(
            int[] widths
    ) {

        int total = 1;

        for (int w : widths) {
            total += w + 3;
        }

        return total;
    }

    private static void printLine(
            int n
    ) {

        System.out.println(
                "-".repeat(n)
        );
    }
}