package com.example.reporting.postgres;

import com.example.postgres.service.PostgresReaderService;

import java.util.ArrayList;
import java.util.List;

public final class PostgresReportService {

    private PostgresReportService() {
    }

    public static void printStoredResults(
            String databaseName,
            List<Integer> queries
    ) {

        List<String> tableNames =
                new ArrayList<>();

        for (Integer query : queries) {

            tableNames.add(
                    "query_" + query
            );
        }

        PostgresReaderService.readAllRowsPretty(
                databaseName,
                tableNames
        );
    }
}