package com.example.config;

import java.io.InputStream;
import java.util.Properties;

public final class ConfigReader {

    private static final String FILE_NAME = "app.properties";
    private static final Properties PROPERTIES = new Properties();

    static {
        loadProperties();
    }

    private ConfigReader() {
        // prevent object creation
    }

    private static void loadProperties() {

        try (InputStream input =
                     ConfigReader.class
                             .getClassLoader()
                             .getResourceAsStream(FILE_NAME)) {

            if (input == null) {
                throw new RuntimeException(FILE_NAME + " not found in resources.");
            }

            PROPERTIES.load(input);

        } catch (Exception e) {
            throw new RuntimeException("Failed to load " + FILE_NAME, e);
        }
    }

    public static String get(String key) {
        return PROPERTIES.getProperty(key);
    }

    public static String get(String key, String defaultValue) {
        return PROPERTIES.getProperty(key, defaultValue);
    }
}