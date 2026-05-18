package com.example.config;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public final class PropertyUpdater {

    // =====================================
    // SOURCE RESOURCE FILE
    // =====================================

    private static final String SOURCE_PATH =
            "common/src/main/resources/app.properties";

    // =====================================
    // RUNTIME CLASSPATH FILE
    // =====================================

    private static final String TARGET_PATH =
            "common/target/classes/app.properties";

    private PropertyUpdater() {
    }

    public static void updateProperty(
            String key,
            String value
    ) {

        updateFile(
                SOURCE_PATH,
                key,
                value
        );

        updateFile(
                TARGET_PATH,
                key,
                value
        );

        System.out.println(
                "\nUpdated app.properties:"
        );

        System.out.println(
                key + "=" + value
        );

        ConfigReader.reload();
    }

    private static void updateFile(
            String filePath,
            String key,
            String value
    ) {

        try {

            Path path =
                    Paths.get(filePath);

            List<String> lines =
                    Files.readAllLines(path);

            List<String> updatedLines =
                    new ArrayList<>();

            boolean updated =
                    false;

            for (String line : lines) {

                String trimmed =
                        line.trim();

                // =====================================
                // UPDATE EXISTING PROPERTY
                // =====================================

                if (trimmed.startsWith(key + "=")) {

                    updatedLines.add(
                            key + "=" + value
                    );

                    updated = true;

                } else {

                    updatedLines.add(line);
                }
            }

            // =====================================
            // APPEND IF NOT FOUND
            // =====================================

            if (!updated) {

                updatedLines.add(
                        key + "=" + value
                );
            }

            Files.write(
                    path,
                    updatedLines
            );

        } catch (Exception e) {

            throw new RuntimeException(
                    "Failed to update file: "
                            + filePath,
                    e
            );
        }
    }
}