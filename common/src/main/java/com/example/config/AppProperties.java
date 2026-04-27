package com.example.config;

import java.io.InputStream;
import java.util.Properties;

public class AppProperties {

    private static final Properties props = new Properties();

    static {
        try (InputStream input =
                     AppProperties.class
                             .getClassLoader()
                             .getResourceAsStream("app.properties")) {

            props.load(input);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String get(String key) {
        return props.getProperty(key);
    }
}