package com.vilkha.config;

import java.io.InputStream;
import java.util.Properties;

public class AppConfig {
    private final Properties props;

    private AppConfig(Properties props) {
        this.props = props;
    }

    public static AppConfig load() {
        Properties p = new Properties();
        try (InputStream is = AppConfig.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (is != null) {
                p.load(is);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load config.properties", e);
        }
        return new AppConfig(p);
    }

    public String get(String key, String defaultValue) {
        String envKey = toEnvKey(key);
        String envVal = System.getenv(envKey);
        if (envVal != null && !envVal.isBlank()) {
            return envVal.trim();
        }

        String v = props.getProperty(key);
        if (v != null && !v.isBlank()) {
            return v.trim();
        }

        return defaultValue;
    }

    private static String toEnvKey(String key) {
        return key.toUpperCase().replace('.', '_');
    }
}
