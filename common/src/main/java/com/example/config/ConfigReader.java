package com.example.config;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public final class ConfigReader {

    private static final String FILE_PATH =
            "common/src/main/resources/app.properties";

    private static final Properties PROPERTIES =
            new Properties();

    static {
        loadProperties();
    }

    private ConfigReader() {
    }

    private static void loadProperties() {

        try (InputStream input =
                     new FileInputStream(FILE_PATH)) {

            PROPERTIES.load(input);

        } catch (Exception e) {

            throw new RuntimeException(
                    "Failed to load app.properties",
                    e
            );
        }
    }

    public static String get(String key) {

        return PROPERTIES.getProperty(key);
    }

    public static String get(
            String key,
            String defaultValue
    ) {

        return PROPERTIES.getProperty(
                key,
                defaultValue
        );
    }

    public static void reload() {

        PROPERTIES.clear();

        loadProperties();

        System.out.println(
                "reload done"
        );
    }
}