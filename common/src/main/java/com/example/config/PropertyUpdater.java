package com.example.config;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public final class PropertyUpdater {

    private static final String APP_PROPERTIES_PATH =
            "common/src/main/resources/app.properties";

    private PropertyUpdater() {
    }

    public static void updateProperty(
            String key,
            String value
    ) {

        try {

            Path path =
                    Paths.get(APP_PROPERTIES_PATH);

            List<String> lines =
                    Files.readAllLines(path);

            List<String> updatedLines =
                    new ArrayList<>();

            boolean updated = false;

            for (String line : lines) {

                String trimmed =
                        line.trim();

                // =====================================
                // REPLACE EXISTING PROPERTY
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

            System.out.println(
                    "\nUpdated app.properties:"
            );

            System.out.println(
                    key + "=" + value
            );
            ConfigReader.reload();

        } catch (Exception e) {

            throw new RuntimeException(
                    "Failed to update app.properties",
                    e
            );
        }
    }
}