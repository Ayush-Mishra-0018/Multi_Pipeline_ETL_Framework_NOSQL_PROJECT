package com.example.input;

import com.example.config.ConfigReader;
import com.example.config.PropertyUpdater;

import java.util.*;

public final class UserInputHandler {

    private static final Scanner scanner =
            new Scanner(System.in);

    private UserInputHandler() {
    }

    public static int getPipelineChoice() {

        System.out.print(
                "\nSelect pipeline number: "
        );

        return scanner.nextInt();
    }

    public static List<Integer> getQueries() {

        System.out.print(
                "\nEnter queries to execute " +
                        "(space separated): "
        );

        scanner.nextLine();

        String input =
                scanner.nextLine();

        String[] tokens =
                input.trim().split("\\s+");

        List<Integer> queries =
                new ArrayList<>();

        for (String token : tokens) {

            try {

                int query =
                        Integer.parseInt(token);

                queries.add(query);

            } catch (Exception e) {

                System.out.println(
                        "Invalid query input: " + token
                );

                System.exit(1);
            }
        }

        validateQueries(queries);

        return queries;
    }

    // =====================================
    // BATCH SIZE INPUT
    // =====================================

    // =====================================
// BATCH SIZE INPUT
// =====================================

    public static int getBatchSize(
            int defaultBatchSize
    ) {

        System.out.print(
                "\nEnter batch size " +
                        "(press Enter to use default = "
                        + defaultBatchSize
                        + "): "
        );

        String input =
                scanner.nextLine().trim();

        // =====================================
        // USE DEFAULT
        // =====================================

        if (input.isEmpty()) {

            System.out.println(
                    "Using default batch size: "
                            + defaultBatchSize
            );

            return defaultBatchSize;
        }

        // =====================================
        // VALIDATE INPUT
        // =====================================

        try {

            int batchSize =
                    Integer.parseInt(input);

            if (batchSize <= 0) {

                throw new RuntimeException();
            }

            return batchSize;

        } catch (Exception e) {

            System.out.println(
                    "Invalid batch size."
            );

            System.exit(1);

            return defaultBatchSize;
        }
    }
    private static void validateQueries(
            List<Integer> queries
    ) {

        // =====================================
        // VALIDATE LENGTH
        // =====================================

        if (queries.size() != 1 &&
                queries.size() != 3) {

            System.out.println(
                    "You must enter either:\n" +
                            "- exactly 1 query\n" +
                            "- or all 3 queries (1 2 3)"
            );

            System.exit(1);
        }

        // =====================================
        // CASE: ONLY 1 QUERY
        // =====================================

        if (queries.size() == 1) {

            int q =
                    queries.get(0);

            if (q != 1 &&
                    q != 2 &&
                    q != 3) {

                System.out.println(
                        "Single query must be one of: 1 2 3"
                );

                System.exit(1);
            }
        }

        // =====================================
        // CASE: ALL 3 QUERIES
        // =====================================

        if (queries.size() == 3) {

            Set<Integer> expected =
                    new HashSet<>(
                            Arrays.asList(1, 2, 3)
                    );

            Set<Integer> actual =
                    new HashSet<>(queries);

            if (!actual.equals(expected)) {

                System.out.println(
                        "When selecting 3 queries, " +
                                "input must be exactly: 1 2 3"
                );

                System.exit(1);
            }
        }
    }
}